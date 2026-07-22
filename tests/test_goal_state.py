from __future__ import annotations

import json

import goal_state


def _goal(status: str, updated_at: int) -> dict:
    return {
        "threadId": "thread-1",
        "objective": "ship reliably",
        "status": status,
        "tokenBudget": None,
        "tokensUsed": 10,
        "timeUsedSeconds": 2,
        "createdAt": 1,
        "updatedAt": updated_at,
    }


def test_goal_snapshot_persists_and_rejects_stale_update(tmp_path):
    path = tmp_path / "goal_snapshots.json"
    goal_state.configure(path)

    assert goal_state.apply_goal_event({
        "type": "goal_update", "session_id": "s1", "goal": _goal("complete", 20),
    })
    assert not goal_state.apply_goal_event({
        "type": "goal_update", "session_id": "s1", "goal": _goal("active", 10),
    })

    goal_state.configure(path)
    snapshot = goal_state.snapshot()

    assert snapshot["revision"] == 1
    assert snapshot["items"] == [{
        "session_id": "s1",
        "goal": _goal("complete", 20),
        "revision": 1,
    }]
    assert not path.with_suffix(".json.tmp").exists()
    assert json.loads(path.read_text())["items"]["s1"]["goal"]["status"] == "complete"


def test_goal_clear_persists_tombstone(tmp_path):
    path = tmp_path / "goal_snapshots.json"
    goal_state.configure(path)
    goal_state.apply_goal_event({
        "type": "goal_update", "session_id": "s1", "goal": _goal("active", 10),
    })
    assert goal_state.apply_goal_event({"type": "goal_cleared", "session_id": "s1"})

    goal_state.configure(path)
    snapshot = goal_state.snapshot()

    assert snapshot["revision"] == 2
    assert snapshot["items"] == [{"session_id": "s1", "goal": None, "revision": 2}]
