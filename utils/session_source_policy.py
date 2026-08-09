"""Shared source-isolation policy for discovered Claude/Codex sessions."""
from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterable


def normalize_globs(raw: object) -> tuple[str, ...]:
    """Return a stable tuple from YAML lists or comma/newline-separated env values."""
    if raw is None:
        return ()
    if isinstance(raw, str):
        values = raw.replace("\n", ",").split(",")
    elif isinstance(raw, Iterable):
        values = [str(value) for value in raw]
    else:
        values = [str(raw)]
    return tuple(value.strip() for value in values if value and value.strip())


def cwd_is_ignored(cwd: str | None, patterns: Iterable[str]) -> bool:
    """Match an expanded, normalized cwd against configured shell-style globs."""
    if not cwd:
        return False
    normalized = os.path.normpath(os.path.expanduser(cwd))
    for raw_pattern in patterns:
        pattern = os.path.normpath(os.path.expanduser(str(raw_pattern).strip()))
        if not pattern:
            continue
        if fnmatch.fnmatchcase(normalized, pattern):
            return True
        # Treat a directory pattern as including all descendants, even when the
        # operator wrote `/private/tmp` instead of `/private/tmp/**`.
        plain_root = pattern.rstrip(os.sep)
        if not any(char in plain_root for char in "*?[") and (
            normalized == plain_root or normalized.startswith(plain_root + os.sep)
        ):
            return True
    return False


def name_is_ignored(name: str | None, prefixes: Iterable[str]) -> bool:
    """Return true when a session title begins with a configured noise prefix."""
    normalized = (name or "").strip()
    return any(
        normalized.startswith(str(prefix).strip())
        for prefix in prefixes
        if str(prefix).strip()
    )


def codex_session_is_ignored(
    cwd: str | None,
    name: str | None,
    cwd_patterns: Iterable[str],
    name_prefixes: Iterable[str],
) -> bool:
    return cwd_is_ignored(cwd, cwd_patterns) or name_is_ignored(name, name_prefixes)


def path_is_within(path: str, root: str) -> bool:
    """Boundary-safe containment check; unlike startswith, rejects sibling prefixes."""
    if not path or not root:
        return False
    try:
        real_path = os.path.realpath(os.path.expanduser(path))
        real_root = os.path.realpath(os.path.expanduser(root))
        return os.path.commonpath((real_path, real_root)) == real_root
    except (OSError, ValueError):
        return False
