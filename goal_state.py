"""Durable latest Goal snapshot used to heal dropped WebSocket events."""
from __future__ import annotations

import copy
import json
import logging
import os
from pathlib import Path
from typing import Any


log = logging.getLogger(__name__)

_path: Path | None = None
_revision = 0
_items: dict[str, dict[str, Any]] = {}


def configure(path: str | os.PathLike[str] | None) -> None:
    global _path, _revision, _items
    _path = Path(path) if path else None
    _revision = 0
    _items = {}
    if _path is None:
        return
    try:
        data = json.loads(_path.read_text(encoding="utf-8"))
        _revision = max(0, int(data.get("revision", 0)))
        raw_items = data.get("items", {})
        if isinstance(raw_items, dict):
            _items = {
                str(session_id): record
                for session_id, record in raw_items.items()
                if isinstance(record, dict)
            }
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("[goal] snapshot load failed: %s", exc)


def _persist() -> None:
    if _path is None:
        return
    try:
        _path.parent.mkdir(parents=True, exist_ok=True)
        temp = _path.with_suffix(_path.suffix + ".tmp")
        temp.write_text(
            json.dumps({"revision": _revision, "items": _items}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.chmod(temp, 0o600)
        os.replace(temp, _path)
    except Exception as exc:
        log.warning("[goal] snapshot persist failed: %s", exc)


def apply_goal_event(event: dict[str, Any]) -> bool:
    global _revision
    event_type = event.get("type")
    if event_type not in {"goal_update", "goal_cleared"}:
        return False
    session_id = event.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return False

    next_goal = copy.deepcopy(event.get("goal")) if event_type == "goal_update" else None
    if event_type == "goal_update" and not isinstance(next_goal, dict):
        return False
    current = _items.get(session_id)
    if current is not None:
        current_goal = current.get("goal")
        if isinstance(current_goal, dict) and isinstance(next_goal, dict):
            if int(current_goal.get("updatedAt", 0) or 0) > int(next_goal.get("updatedAt", 0) or 0):
                return False
        if current_goal == next_goal:
            return False

    _revision += 1
    _items[session_id] = {"goal": next_goal, "revision": _revision}
    _persist()
    log.info(
        "[goal] snapshot session=%s status=%s revision=%d",
        session_id,
        next_goal.get("status") if isinstance(next_goal, dict) else "cleared",
        _revision,
    )
    return True


def snapshot() -> dict[str, Any]:
    return {
        "type": "goals_snapshot",
        "revision": _revision,
        "items": [
            {
                "session_id": session_id,
                "goal": copy.deepcopy(record.get("goal")),
                "revision": int(record.get("revision", 0)),
            }
            for session_id, record in sorted(_items.items())
        ],
    }
