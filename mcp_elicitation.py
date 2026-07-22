"""Codex MCP elicitation normalization and browser-origin policy.

The Codex app-server forwards MCP ``elicitation/create`` requests to its
client.  Browser Use relies on that channel for per-origin consent.  This
module keeps the policy and wire-shape conversion independent from the
app-server transport so it can be tested without launching Codex or Chrome.
"""
from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit


_VALID_BROWSER_MODES = frozenset({"ask", "allowlist", "allow_all", "deny"})
_BROWSER_ORIGIN_TOOL = "access_browser_origin"
_DECISION_QUESTION_ID = "decision"
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _mapping(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _walk_mappings(value: Any, *, max_depth: int = 8):
    if max_depth < 0:
        return
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_mappings(child, max_depth=max_depth - 1)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_mappings(child, max_depth=max_depth - 1)


def find_value(payload: Any, key: str) -> Any:
    """Find a metadata field even when an MCP adapter nests ``_meta``."""
    for item in _walk_mappings(payload):
        if key in item:
            return item[key]
    return None


def browser_origin_request(params: dict) -> tuple[str, str] | None:
    """Return ``(tool_name, origin)`` for Browser Use approval elicitations."""
    tool_name = find_value(params, "tool_name")
    origin = find_value(params, "origin")
    connector_id = find_value(params, "connector_id")
    if connector_id != "browser-use" or not isinstance(tool_name, str):
        return None
    if not isinstance(origin, str) or not origin.strip():
        return None
    return tool_name.strip(), origin.strip()


def _normalized_origin(value: str) -> str | None:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    if parsed.username or parsed.password:
        return None
    host = parsed.hostname.lower().rstrip(".")
    try:
        port = parsed.port
    except ValueError:
        return None
    default_port = (parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)
    suffix = "" if port is None or default_port else f":{port}"
    return f"{parsed.scheme}://{host}{suffix}"


def _host_matches(host: str, pattern: str) -> bool:
    candidate = pattern.strip().lower().rstrip(".")
    if not candidate:
        return False
    if candidate.startswith("*."):
        suffix = candidate[2:]
        return bool(suffix) and host != suffix and host.endswith(f".{suffix}")
    return host == candidate


def origin_matches(origin: str, pattern: str) -> bool:
    normalized = _normalized_origin(origin)
    if normalized is None:
        return False
    parsed = urlsplit(normalized)
    candidate = pattern.strip()
    if not candidate:
        return False

    if "://" not in candidate:
        host_pattern, sep, port = candidate.partition(":")
        if not _host_matches(parsed.hostname or "", host_pattern):
            return False
        return not sep or str(parsed.port or (443 if parsed.scheme == "https" else 80)) == port

    pattern_parts = urlsplit(candidate)
    if pattern_parts.scheme not in {"http", "https"} or pattern_parts.scheme != parsed.scheme:
        return False
    if not _host_matches(parsed.hostname or "", pattern_parts.hostname or ""):
        return False
    try:
        pattern_port = pattern_parts.port
    except ValueError:
        return False
    if pattern_port is None:
        return True
    return parsed.port == pattern_port


@dataclass(frozen=True)
class ElicitationDecision:
    action: str
    content: Any = None
    meta: dict | None = None
    reason: str = ""

    def to_rpc_result(self) -> dict:
        return {"action": self.action, "content": self.content, "_meta": self.meta}


@dataclass(frozen=True)
class BrowserOriginPolicy:
    mode: str = "ask"
    allowed_origins: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "BrowserOriginPolicy":
        raw_mode = os.environ.get("BRIDGE_BROWSER_ORIGIN_MODE", "ask").strip().lower()
        mode = raw_mode if raw_mode in _VALID_BROWSER_MODES else "ask"
        raw_origins = os.environ.get("BRIDGE_BROWSER_ALLOWED_ORIGINS", "")
        allowed = tuple(part.strip() for part in raw_origins.split(",") if part.strip())
        return cls(mode=mode, allowed_origins=allowed)

    def decide(self, params: dict) -> ElicitationDecision | None:
        request = browser_origin_request(params)
        if request is None:
            return None
        tool_name, origin = request

        # Only ordinary origin access may be automated. Raw CDP, history,
        # uploads/downloads, credentials and other Browser Use approvals must
        # remain explicit user interactions.
        if tool_name != _BROWSER_ORIGIN_TOOL:
            return None
        if _normalized_origin(origin) is None:
            return ElicitationDecision("decline", reason="invalid browser origin")
        if self.mode == "deny":
            return ElicitationDecision("decline", reason="browser origin policy denies access")
        if self.mode == "allow_all":
            return ElicitationDecision(
                "accept",
                meta={"persist": "session"},
                reason="browser origin policy allows all valid HTTP(S) origins",
            )
        if self.mode == "allowlist" and any(origin_matches(origin, item) for item in self.allowed_origins):
            return ElicitationDecision(
                "accept",
                meta={"persist": "session"},
                reason="browser origin matched allowlist",
            )
        return None


def ensure_browser_elicitation_routing(codex_home: str | None = None) -> bool:
    """Route Browser Use consent to the app-server client, not auto-review."""
    manage = os.environ.get("BRIDGE_BROWSER_MANAGE_AUTO_REVIEW", "1").strip().lower()
    if manage in _FALSE_VALUES:
        return False
    root = codex_home or os.environ.get("CODEX_HOME") or os.path.expanduser("~/.codex")
    path = os.path.join(root, "browser", "config.toml")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            original = handle.read()
    except FileNotFoundError:
        original = ""

    pattern = re.compile(r"(?m)^[ \t]*disable_auto_review[ \t]*=.*$")
    updated = pattern.sub("disable_auto_review = true", original, count=1)
    if updated == original and not pattern.search(original):
        updated = "disable_auto_review = true\n" + original
    if updated == original:
        return False

    fd, temporary = tempfile.mkstemp(prefix=".config.toml.", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        if os.path.exists(path):
            os.chmod(temporary, os.stat(path).st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return True


def _enum_options(schema: dict) -> list[dict]:
    values = schema.get("enum")
    names = schema.get("enumNames")
    if isinstance(values, list):
        return [
            {
                "id": str(value),
                "label": str(names[index]) if isinstance(names, list) and index < len(names) else str(value),
                "description": "",
                "recommended": value == schema.get("default"),
            }
            for index, value in enumerate(values)
        ]
    for key in ("oneOf", "anyOf"):
        choices = schema.get(key)
        if isinstance(choices, list):
            result: list[dict] = []
            for choice in choices:
                item = _mapping(choice)
                value = item.get("const", item.get("value", item.get("title", "")))
                result.append({
                    "id": str(value),
                    "label": str(item.get("title") or value),
                    "description": str(item.get("description") or ""),
                    "recommended": value == schema.get("default"),
                })
            return result
    return []


def elicitation_questions(params: dict) -> list[dict]:
    """Convert MCP form/openai-form payloads to the bridge question shape."""
    browser_request = browser_origin_request(params)
    if browser_request is not None:
        tool_name, origin = browser_request
        if tool_name != _BROWSER_ORIGIN_TOOL:
            return [{
                "question_id": _DECISION_QUESTION_ID,
                "text": str(params.get("message") or f"Allow this sensitive Chrome action on {origin}?"),
                "header": "Chrome 高風險授權",
                "type": "choice",
                "options": [
                    {"id": "deny", "label": "拒絕", "description": origin, "recommended": True},
                    {"id": "approve_once", "label": "僅允許這次", "description": origin, "recommended": False},
                ],
                "multi_select": False,
                "free_form": False,
            }]
        return [{
            "question_id": _DECISION_QUESTION_ID,
            "text": str(params.get("message") or f"Allow Chrome to access {origin}?"),
            "header": "Chrome 網域授權",
            "type": "choice",
            "options": [
                {"id": "approve_once", "label": "允許這次", "description": origin, "recommended": True},
                {"id": "approve_always", "label": "總是允許", "description": origin, "recommended": False},
                {"id": "deny", "label": "拒絕", "description": origin, "recommended": False},
            ],
            "multi_select": False,
            "free_form": False,
        }]

    schema = _mapping(params.get("requestedSchema"))
    properties = _mapping(schema.get("properties"))
    required = set(schema.get("required") or [])
    questions: list[dict] = []
    for key, raw_property in properties.items():
        prop = _mapping(raw_property)
        prop_type = str(prop.get("type") or "string")
        options = _enum_options(prop)
        multi = prop_type == "array"
        if multi:
            item_schema = _mapping(prop.get("items"))
            options = _enum_options(item_schema) or _enum_options(prop)
        if prop_type == "boolean" and not options:
            options = [
                {"id": "true", "label": "是", "description": "", "recommended": prop.get("default") is True},
                {"id": "false", "label": "否", "description": "", "recommended": prop.get("default") is False},
            ]
        questions.append({
            "question_id": str(key),
            "text": str(prop.get("description") or prop.get("title") or key),
            "header": str(prop.get("title") or ""),
            "type": "multi_choice" if multi else ("choice" if options else "question"),
            "options": options,
            "multi_select": multi,
            "free_form": not options,
            "required": key in required,
            "schema_type": prop_type,
        })
    if questions:
        return questions
    return [{
        "question_id": _DECISION_QUESTION_ID,
        "text": str(params.get("message") or "Allow this external tool request?"),
        "header": "外部工具授權",
        "type": "choice",
        "options": [
            {"id": "deny", "label": "拒絕", "description": "", "recommended": True},
            {"id": "approve_once", "label": "允許這次", "description": "", "recommended": False},
        ],
        "multi_select": False,
        "free_form": False,
    }]


def _coerce_answer(value: Any, schema: dict) -> Any:
    schema_type = schema.get("type")
    if schema_type == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}
    if schema_type == "integer":
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if schema_type == "number":
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if schema_type == "array":
        return value if isinstance(value, list) else ([] if value in (None, "") else [value])
    return value


def elicitation_response(params: dict, response: dict) -> dict:
    """Build a Codex ``McpServerElicitationRequestResponse``."""
    if response.get("cancelled") or response.get("canceled"):
        return {"action": "cancel", "content": None, "_meta": None}
    answers = response.get("answers") if isinstance(response.get("answers"), dict) else {}
    decision = answers.get(_DECISION_QUESTION_ID)
    if decision == "deny":
        return {"action": "decline", "content": None, "_meta": None}
    if decision in {"approve_once", "approve_always"}:
        persist = "always" if decision == "approve_always" else "session"
        return {"action": "accept", "content": None, "_meta": {"persist": persist}}

    schema = _mapping(params.get("requestedSchema"))
    properties = _mapping(schema.get("properties"))
    content = {
        key: _coerce_answer(value, _mapping(properties.get(key)))
        for key, value in answers.items()
        if key in properties
    }
    return {"action": "accept", "content": content, "_meta": None}
