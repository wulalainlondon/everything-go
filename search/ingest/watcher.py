"""
watcher.py — watchdog-based file watcher for JSONL source directories.

Per TECH_RESEARCH Q3:
- macOS: PollingObserver; the process already has an FSEvents JSONL watcher,
  and watchdog's native FSEvents extension rejects duplicate path watches
- Linux: InotifyObserver; log inotify watch count at startup
- Windows: WindowsApiObserver
- Any import failure → PollingObserver(interval=config.watch_interval_sec)

Coalesces duplicate events for the same path within 500 ms.
Only emits events for .jsonl files.
"""
from __future__ import annotations

import asyncio
import logging
import platform
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..sources.base import SearchSource

log = logging.getLogger(__name__)

_COALESCE_MS = 0.5  # 500 ms coalesce window


# ---------------------------------------------------------------------------
# Observer factory — per platform, with fallback chain
# ---------------------------------------------------------------------------

def _make_observer(poll_interval: int = 2):
    """Return the best available watchdog Observer for the current platform."""
    _sys = platform.system()

    if _sys == 'Darwin':
        # KqueueObserver opens every filesystem entry. A second
        # FSEventsObserver cannot watch a path already owned by jsonl_sessions
        # in the same process. Polling is bounded and search indexing can
        # tolerate its configured delay.
        try:
            from watchdog.observers.polling import PollingObserver  # type: ignore[import-untyped]
            interval = max(2, poll_interval)
            log.info("WatchdogWatcher: using PollingObserver (macOS, interval=%ds)", interval)
            return PollingObserver(timeout=interval)
        except Exception as exc:
            raise RuntimeError(f"PollingObserver unavailable on macOS: {exc}") from exc

    elif _sys == 'Linux':
        try:
            from watchdog.observers.inotify import InotifyObserver  # type: ignore[import-untyped]
            _log_inotify_limits()
            log.info("WatchdogWatcher: using InotifyObserver (Linux)")
            return InotifyObserver()
        except Exception as exc:
            log.warning("WatchdogWatcher: InotifyObserver unavailable (%s), falling back to polling", exc)

    elif _sys == 'Windows':
        try:
            from watchdog.observers.winapi import WindowsApiObserver  # type: ignore[import-untyped]
            log.info("WatchdogWatcher: using WindowsApiObserver (Windows)")
            return WindowsApiObserver()
        except Exception as exc:
            log.warning("WatchdogWatcher: WindowsApiObserver unavailable (%s), falling back to polling", exc)

    # Final fallback: PollingObserver
    try:
        from watchdog.observers.polling import PollingObserver  # type: ignore[import-untyped]
        log.info("WatchdogWatcher: using PollingObserver (interval=%ds)", poll_interval)
        return PollingObserver(timeout=poll_interval)
    except Exception as exc:
        raise RuntimeError(f"No watchdog Observer available: {exc}") from exc


def _log_inotify_limits() -> None:
    """Log current inotify watch count and max on Linux (informational)."""
    try:
        max_watches_path = Path('/proc/sys/fs/inotify/max_user_watches')
        current_path = Path('/proc/sys/fs/inotify/max_user_instances')
        max_watches = max_watches_path.read_text().strip() if max_watches_path.exists() else 'unknown'
        log.info("WatchdogWatcher: inotify max_user_watches=%s", max_watches)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------------

class _JsonlEventHandler:
    """watchdog FileSystemEventHandler that puts .jsonl paths into an asyncio.Queue."""

    def __init__(
        self,
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
        path_filter=None,
    ):
        self._queue = queue
        self._loop = loop
        self._path_filter = path_filter
        # Coalesce: path_str → scheduled_time
        self._pending: Dict[str, float] = {}
        # Overflow: paths dropped due to QueueFull, retried on next enqueue opportunity
        self._overflow: set = set()

    def _schedule(self, path_str: str) -> None:
        """Thread-safe: called from watchdog thread, enqueues into asyncio loop."""
        if not path_str.endswith('.jsonl'):
            return
        if self._path_filter is not None and not self._path_filter(Path(path_str)):
            return
        now = time.monotonic()
        self._pending[path_str] = now + _COALESCE_MS
        # Schedule the actual enqueue after coalesce window
        self._loop.call_soon_threadsafe(self._enqueue_after_coalesce, path_str, now)

    def _enqueue_after_coalesce(self, path_str: str, scheduled_at: float) -> None:
        """Runs in asyncio thread; checks coalesce window before enqueuing."""
        # Drain overflow set first: attempt to push any previously-dropped paths.
        # Iteration over a copy so we can mutate the set safely.
        for overflow_path in list(self._overflow):
            try:
                self._queue.put_nowait(Path(overflow_path))
                self._overflow.discard(overflow_path)
            except asyncio.QueueFull:
                break  # Queue still full; leave remainder in overflow for next round

        scheduled_fire = self._pending.get(path_str)
        if scheduled_fire is None:
            return
        now = time.monotonic()
        if now < scheduled_fire:
            # Not ready yet; re-schedule
            delay = scheduled_fire - now
            self._loop.call_later(delay, self._enqueue_after_coalesce, path_str, scheduled_at)
            return
        # Coalesce window elapsed — emit
        del self._pending[path_str]
        try:
            self._queue.put_nowait(Path(path_str))
        except asyncio.QueueFull:
            self._overflow.add(path_str)
            log.warning("WatchdogWatcher: queue full, buffering event for %s (overflow size: %d)",
                        path_str, len(self._overflow))

    # watchdog callback interface (duck-typed, no inheritance needed for testing)
    def dispatch(self, event) -> None:
        src = getattr(event, 'src_path', None)
        if src:
            self._schedule(src)
        # Also handle moves (destination path)
        dest = getattr(event, 'dest_path', None)
        if dest:
            self._schedule(dest)

    # Individual event methods (watchdog calls these via dispatch in standard handler,
    # but we override dispatch directly for simplicity)
    def on_created(self, event) -> None:
        self._schedule(event.src_path)

    def on_modified(self, event) -> None:
        self._schedule(event.src_path)

    def on_moved(self, event) -> None:
        self._schedule(event.dest_path)

    def on_deleted(self, event) -> None:
        pass  # Deletion doesn't require re-ingest


# ---------------------------------------------------------------------------
# WatchdogWatcher
# ---------------------------------------------------------------------------

class WatchdogWatcher:
    """
    Watches all enabled source root directories for .jsonl changes.
    Emits Path objects into an asyncio.Queue after 500 ms coalesce.
    """

    def __init__(
        self,
        sources: List,
        queue: asyncio.Queue,
        poll_interval: int = 2,
    ):
        self._sources = sources
        self._queue = queue
        self._poll_interval = poll_interval
        self._observer = None
        self._handler: Optional[_JsonlEventHandler] = None
        self._watch_dirs: List[Path] = []

    def _collect_watch_dirs(self) -> List[Path]:
        """Derive watch root directories from enabled sources."""
        dirs: List[Path] = []
        for source in self._sources:
            if not source.is_enabled():
                continue
            roots = getattr(source, 'watch_roots', None)
            if roots is not None:
                for root in roots:
                    if root.exists() and root not in dirs:
                        dirs.append(root)
                continue
            # Use the source's own watch_root attribute so config overrides take effect.
            root = getattr(source, 'watch_root', None)
            if root is None:
                log.warning("WatchdogWatcher: source %s has no watch_root, skipping", source.name)
                continue
            if root.exists():
                dirs.append(root)
        return dirs

    def start(self) -> None:
        """Start the file watcher. Safe to call from asyncio thread (runs watchdog in its own thread)."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()

        self._observer = _make_observer(self._poll_interval)
        def _path_is_in_scope(path: Path) -> bool:
            for source in self._sources:
                predicate = getattr(source, 'should_index_path', None)
                if predicate is not None:
                    try:
                        if predicate(path):
                            return True
                    except Exception:
                        continue
                elif path.is_relative_to(getattr(source, 'watch_root', path.parent)):
                    return True
            return False

        self._handler = _JsonlEventHandler(
            self._queue,
            loop,
            path_filter=_path_is_in_scope,
        )

        self._watch_dirs = self._collect_watch_dirs()
        if not self._watch_dirs:
            log.info("WatchdogWatcher: no watch directories found, watcher idle")

        for watch_dir in self._watch_dirs:
            try:
                self._observer.schedule(self._handler, str(watch_dir), recursive=True)
                log.info("WatchdogWatcher: watching %s", watch_dir)
            except Exception as exc:
                log.warning("WatchdogWatcher: cannot watch %s: %s", watch_dir, exc)

        self._observer.start()

    def stop(self) -> None:
        """Stop the watcher thread. Safe to call multiple times."""
        if self._observer is not None:
            try:
                self._observer.stop()
                self._observer.join(timeout=5)
            except Exception as exc:
                log.warning("WatchdogWatcher: error stopping observer: %s", exc)
            finally:
                self._observer = None
