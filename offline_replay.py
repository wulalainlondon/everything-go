"""Reliable offline event replay for reconnecting clients."""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
from typing import Any, Iterable

import client_manager


log = logging.getLogger(__name__)

REPLAY_BATCH_SIZE = 64
REPLAY_ACK_TIMEOUT_SECONDS = 10.0

_replay_lock: asyncio.Lock | None = None
_pending_acks: dict[tuple[int, str], asyncio.Future[None]] = {}


def _lock() -> asyncio.Lock:
    global _replay_lock
    if _replay_lock is None:
        _replay_lock = asyncio.Lock()
    return _replay_lock


def _ws_key(ws: Any, batch_id: str) -> tuple[int, str]:
    return id(client_manager.unwrap_ws(ws)), batch_id


def ack_offline_replay(ws: Any, batch_id: str) -> bool:
    """Resolve the exact in-flight application ACK for this connection."""
    if not batch_id:
        return False
    future = _pending_acks.get(_ws_key(ws, batch_id))
    if future is None or future.done():
        return False
    future.set_result(None)
    return True


def _take_batch(sessions: list[Any]) -> list[tuple[Any, dict]]:
    batch: list[tuple[Any, dict]] = []
    for session in sessions:
        for event in session.offline_buffer:
            batch.append((session, event))
            if len(batch) >= REPLAY_BATCH_SIZE:
                return batch
    return batch


def _event_key(event: dict) -> tuple[Any, ...]:
    return (
        event.get("gen"),
        event.get("seq"),
        event.get("session_id"),
        event.get("type"),
    )


def _commit_batch(entries: list[tuple[Any, dict]]) -> int:
    """Remove only ACKed events, tolerating concurrent tail appends/coalescing."""
    by_session: dict[int, tuple[Any, list[dict]]] = {}
    for session, event in entries:
        item = by_session.setdefault(id(session), (session, []))
        item[1].append(event)

    committed = 0
    for session, events in by_session.values():
        for event in events:
            found = next(
                (index for index, current in enumerate(session.offline_buffer) if current is event),
                None,
            )
            if found is None:
                key = _event_key(event)
                found = next(
                    (
                        index for index, current in enumerate(session.offline_buffer)
                        if _event_key(current) == key and (key[0] is not None or current == event)
                    ),
                    None,
                )
            if found is not None:
                del session.offline_buffer[found]
                committed += 1
    return committed


def _remaining(sessions: list[Any]) -> int:
    return sum(len(session.offline_buffer) for session in sessions)


async def _replay_with_ack(ws: Any, sessions: list[Any], client: Any = None) -> int:
    replayed = 0
    while True:
        entries = _take_batch(sessions)
        if not entries:
            return replayed

        batch_id = secrets.token_hex(8)
        key = _ws_key(ws, batch_id)
        future = asyncio.get_running_loop().create_future()
        _pending_acks[key] = future
        payload = {
            "type": "offline_replay_batch",
            "batch_id": batch_id,
            "events": [event for _, event in entries],
            "remaining": max(0, _remaining(sessions) - len(entries)),
        }
        log.info(
            "[replay] send batch=%s client=%s count=%d remaining=%d",
            batch_id,
            getattr(client, "client_id", "<unknown>"),
            len(entries),
            payload["remaining"],
        )
        try:
            while True:
                if not await client_manager.send_json(ws, payload, client):
                    return replayed
                try:
                    await asyncio.wait_for(
                        asyncio.shield(future),
                        timeout=REPLAY_ACK_TIMEOUT_SECONDS,
                    )
                    break
                except asyncio.TimeoutError:
                    log.warning(
                        "[replay] ack timeout; resend batch=%s client=%s count=%d",
                        batch_id,
                        getattr(client, "client_id", "<unknown>"),
                        len(entries),
                    )

            committed = _commit_batch(entries)
            replayed += committed
            log.info(
                "[replay] ack batch=%s client=%s committed=%d remaining=%d",
                batch_id,
                getattr(client, "client_id", "<unknown>"),
                committed,
                _remaining(sessions),
            )
        finally:
            _pending_acks.pop(key, None)
            if not future.done():
                future.cancel()


async def _replay_legacy(ws: Any, sessions: list[Any], client: Any = None) -> int:
    """Preserve the old wire shape for clients that cannot acknowledge batches."""
    replayed = 0
    for session in sessions:
        if not session.offline_buffer:
            continue
        snapshot = session.offline_buffer[:]
        sent_count = await client_manager.send_text_batch(
            ws,
            [json.dumps(event) for event in snapshot],
            client,
        )
        replayed += sent_count
        del session.offline_buffer[:sent_count]
        if sent_count < len(snapshot):
            return replayed
    return replayed


async def replay_offline_buffers(
    ws: Any,
    sessions: Iterable[Any],
    *,
    supports_ack: bool = False,
    client: Any = None,
) -> int:
    """Replay buffered events, committing ACK-capable batches only after apply."""
    scoped_sessions = list(sessions)
    async with _lock():
        try:
            if supports_ack:
                return await _replay_with_ack(ws, scoped_sessions, client)
            return await _replay_legacy(ws, scoped_sessions, client)
        except asyncio.CancelledError:
            log.info(
                "[replay] release unacked client=%s",
                getattr(client, "client_id", "<unknown>"),
            )
            raise


def reset_for_tests() -> None:
    """Clear loop-bound replay state between isolated asyncio.run tests."""
    global _replay_lock
    for future in _pending_acks.values():
        if not future.done():
            future.cancel()
    _pending_acks.clear()
    _replay_lock = None
