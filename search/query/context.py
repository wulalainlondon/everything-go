"""
Read-only context viewer for a search hit.

Fetches the target message plus up to `around` messages before/after it
within the same session, using rowid-based window (no cross-session bleed).

Usage:
    from search.query.context import get_search_context, SearchContextResponse
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional

from .connection_pool import ConnectionPool


@dataclass
class ContextMessage:
    msg_uuid: str
    role: str
    ts: str
    content: str
    is_target: bool  # True only for the message the user clicked


@dataclass
class SearchContextResponse:
    session_id: str
    session_display_name: Optional[str]
    cwd: Optional[str]
    backend: Optional[str]
    target_msg_uuid: str
    messages: list[ContextMessage]  # ordered by rowid ascending
    elapsed_ms: float


def _run_get_context(
    conn,
    session_id: str,
    msg_uuid: str,
    around: int,
    root_dir: Optional[str],
) -> tuple[Optional[dict], list[tuple]]:
    """
    Synchronous part — runs in asyncio.to_thread.

    Returns:
        (session_row_dict_or_None, message_rows)

    message_rows columns: (msg_uuid, role, ts, content)
    """
    # 1. Look up session metadata
    sess_row = conn.execute(
        "SELECT display_name, cwd, backend FROM sessions WHERE session_id = ? LIMIT 1",
        (session_id,),
    ).fetchone()
    if root_dir is not None:
        cwd = sess_row[1] if sess_row else ""
        root = root_dir.rstrip("/")
        try:
            real_cwd = os.path.realpath(os.path.expanduser(cwd or ""))
            real_root = os.path.realpath(os.path.expanduser(root))
        except Exception:
            return sess_row, []
        if real_cwd != real_root and not real_cwd.startswith(real_root + os.sep):
            return sess_row, []

    # 2. Find the rowid of the target message
    target_row = conn.execute(
        "SELECT rowid FROM messages WHERE session_id = ? AND msg_uuid = ? LIMIT 1",
        (session_id, msg_uuid),
    ).fetchone()

    if target_row is None:
        return sess_row, []

    target_rowid: int = target_row[0]
    lo = target_rowid - around
    hi = target_rowid + around

    rows = conn.execute(
        """
        SELECT msg_uuid, role, ts, content
        FROM messages
        WHERE session_id = ?
          AND rowid BETWEEN ? AND ?
        ORDER BY rowid
        """,
        (session_id, lo, hi),
    ).fetchall()

    return sess_row, rows


async def get_search_context(
    pool: ConnectionPool,
    session_id: str,
    msg_uuid: str,
    around: int = 10,
    root_dir: Optional[str] = None,
) -> SearchContextResponse:
    """
    Return up to `around*2 + 1` messages centred on `msg_uuid` within `session_id`.

    If the target message is not found, returns an empty messages list.
    """
    t0 = time.monotonic()

    async with pool.borrow() as conn:
        sess_row, rows = await asyncio.to_thread(
            _run_get_context, conn, session_id, msg_uuid, around, root_dir
        )

    display_name: Optional[str] = sess_row[0] if sess_row else None
    cwd: Optional[str] = sess_row[1] if sess_row else None
    backend: Optional[str] = sess_row[2] if sess_row else None

    messages = [
        ContextMessage(
            msg_uuid=row[0],
            role=row[1],
            ts=row[2],
            content=row[3],
            is_target=(row[0] == msg_uuid),
        )
        for row in rows
    ]

    elapsed_ms = (time.monotonic() - t0) * 1000

    return SearchContextResponse(
        session_id=session_id,
        session_display_name=display_name,
        cwd=cwd,
        backend=backend,
        target_msg_uuid=msg_uuid,
        messages=messages,
        elapsed_ms=elapsed_ms,
    )
