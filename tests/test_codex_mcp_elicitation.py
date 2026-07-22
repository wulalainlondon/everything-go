from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock


_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_BRIDGE_ROOT))


def _browser_params(origin: str, *, tool_name: str = "access_browser_origin") -> dict:
    return {
        "threadId": "thread_1",
        "turnId": "turn_1",
        "serverName": "node_repl",
        "mode": "openai/form",
        "message": f"Allow Chrome to access {origin}?",
        "requestedSchema": {},
        "_meta": {
            "codex_approval_kind": "mcp_tool_call",
            "connector_id": "browser-use",
            "connector_name": "Chrome",
            "persist": "always",
            "tool_name": tool_name,
            "tool_title": "Access browser origin",
            "tool_params": {"origin": origin},
            "origin": origin,
        },
    }


def _fake_stdin():
    stdin = MagicMock()
    stdin.write = MagicMock()
    stdin.drain = AsyncMock()
    return stdin


def _last_rpc(stdin: MagicMock) -> dict:
    return json.loads(stdin.write.call_args.args[0].decode())


def test_browser_origin_policy_allowlist_and_wildcards():
    from mcp_elicitation import BrowserOriginPolicy

    policy = BrowserOriginPolicy(
        mode="allowlist",
        allowed_origins=("https://studio.youtube.com", "*.canva.com"),
    )

    youtube = policy.decide(_browser_params("https://studio.youtube.com"))
    canva = policy.decide(_browser_params("https://www.canva.com"))

    assert youtube is not None and youtube.action == "accept"
    assert youtube.meta == {"persist": "session"}
    assert canva is not None and canva.action == "accept"
    assert policy.decide(_browser_params("https://youtube.com")) is None
    assert policy.decide(_browser_params("https://canva.com")) is None


def test_browser_origin_policy_allow_all_still_requires_raw_cdp_confirmation():
    from mcp_elicitation import BrowserOriginPolicy, elicitation_questions

    policy = BrowserOriginPolicy(mode="allow_all")

    assert policy.decide(_browser_params("https://example.com")) is not None
    assert policy.decide(
        _browser_params("https://example.com", tool_name="access_browser_origin_with_raw_cdp")
    ) is None

    questions = elicitation_questions(
        _browser_params("https://example.com", tool_name="access_browser_origin_with_raw_cdp")
    )
    assert [option["id"] for option in questions[0]["options"]] == ["deny", "approve_once"]


def test_browser_origin_policy_rejects_non_http_origin():
    from mcp_elicitation import BrowserOriginPolicy

    decision = BrowserOriginPolicy(mode="allow_all").decide(_browser_params("file:///private/data"))

    assert decision is not None
    assert decision.action == "decline"


def test_generic_form_answers_are_typed():
    from mcp_elicitation import elicitation_questions, elicitation_response

    params = {
        "mode": "form",
        "message": "Configure export",
        "requestedSchema": {
            "type": "object",
            "required": ["count"],
            "properties": {
                "count": {"type": "integer", "title": "Count"},
                "enabled": {"type": "boolean", "title": "Enabled"},
                "formats": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["png", "jpg"]},
                },
            },
        },
    }

    questions = elicitation_questions(params)
    result = elicitation_response(params, {
        "answers": {"count": "3", "enabled": "true", "formats": ["png"]},
    })

    assert [question["question_id"] for question in questions] == ["count", "enabled", "formats"]
    assert result == {
        "action": "accept",
        "content": {"count": 3, "enabled": True, "formats": ["png"]},
        "_meta": None,
    }


def test_codex_auto_approves_browser_origin(monkeypatch):
    from backends.codex_appserver import CodexAppServerBackend

    async def run():
        monkeypatch.setenv("BRIDGE_BROWSER_ORIGIN_MODE", "allow_all")
        backend = CodexAppServerBackend("codex")
        proc = MagicMock()
        proc.stdin = _fake_stdin()
        backend._proc = proc

        await backend._handle_server_request(41, "mcpServer/elicitation/request", _browser_params("https://example.com"))
        return _last_rpc(proc.stdin)

    assert asyncio.run(run()) == {
        "id": 41,
        "result": {"action": "accept", "content": None, "_meta": {"persist": "session"}},
    }


def test_codex_forwards_browser_origin_to_mobile_and_returns_choice(monkeypatch):
    from backends.codex_appserver import CodexAppServerBackend
    from interactions import REGISTRY

    async def run():
        monkeypatch.setenv("BRIDGE_BROWSER_ORIGIN_MODE", "ask")
        broadcasts: list[dict] = []

        async def broadcast(payload: dict) -> int:
            broadcasts.append(payload)
            return 1

        backend = CodexAppServerBackend("codex", broadcast_fn=broadcast)
        proc = MagicMock()
        proc.stdin = _fake_stdin()
        backend._proc = proc
        session = MagicMock()
        session.session_id = "s_codex"
        backend._thread_to_session["thread_1"] = session

        await backend._handle_server_request(
            42,
            "mcpServer/elicitation/request",
            _browser_params("https://example.com"),
        )
        request = broadcasts[-1]
        assert request["type"] == "user_input_request"
        assert request["kind"] == "mcp_elicitation"
        assert request["questions"][0]["question_id"] == "decision"

        await REGISTRY.resolve({
            "request_id": request["request_id"],
            "answers": {"decision": "approve_once"},
        })
        return _last_rpc(proc.stdin)

    assert asyncio.run(run()) == {
        "id": 42,
        "result": {"action": "accept", "content": None, "_meta": {"persist": "session"}},
    }


def test_codex_declines_elicitation_without_mobile_client(monkeypatch):
    from backends.codex_appserver import CodexAppServerBackend

    async def run():
        monkeypatch.setenv("BRIDGE_BROWSER_ORIGIN_MODE", "ask")
        backend = CodexAppServerBackend("codex")
        proc = MagicMock()
        proc.stdin = _fake_stdin()
        backend._proc = proc

        await backend._handle_server_request(43, "mcpServer/elicitation/request", _browser_params("https://example.com"))
        return _last_rpc(proc.stdin)

    assert asyncio.run(run()) == {
        "id": 43,
        "result": {"action": "decline", "content": None, "_meta": None},
    }


def test_browser_elicitation_routing_config_is_atomic_and_idempotent(tmp_path, monkeypatch):
    from mcp_elicitation import ensure_browser_elicitation_routing

    config = tmp_path / "browser" / "config.toml"
    config.parent.mkdir()
    config.write_text('[origins]\nallowed = ["https://example.com"]\n')

    assert ensure_browser_elicitation_routing(str(tmp_path)) is True
    assert config.read_text() == (
        'disable_auto_review = true\n[origins]\nallowed = ["https://example.com"]\n'
    )
    assert ensure_browser_elicitation_routing(str(tmp_path)) is False

    monkeypatch.setenv("BRIDGE_BROWSER_MANAGE_AUTO_REVIEW", "0")
    config.write_text("disable_auto_review = false\n")
    assert ensure_browser_elicitation_routing(str(tmp_path)) is False
    assert config.read_text() == "disable_auto_review = false\n"
