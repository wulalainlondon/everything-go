from __future__ import annotations

from pathlib import Path

from backends.history import BoundedHistoryCache, HistoryIndex
import jsonl_sessions
from search.sources import claude as claude_source
from search.sources.claude import ClaudeJsonlSource


def _index(name: str, size: int) -> HistoryIndex:
    return HistoryIndex(
        key=(name, 1, size),
        built_at=1.0,
        messages=[{"content": name}],
    )


def test_history_cache_evicts_lru_by_entry_count() -> None:
    cache = BoundedHistoryCache(max_entries=2, max_bytes=1024 * 1024)
    cache["a"] = _index("a", 4096)
    cache["b"] = _index("b", 4096)
    assert cache.get("a") is not None  # promote a

    cache["c"] = _index("c", 4096)

    assert set(cache) == {"a", "c"}
    assert cache.bytes_used == 8192


def test_history_cache_enforces_total_bytes_and_rejects_oversized_entry() -> None:
    cache = BoundedHistoryCache(max_entries=8, max_bytes=10_000)
    cache["a"] = _index("a", 6000)
    cache["b"] = _index("b", 6000)
    assert list(cache) == ["b"]
    assert cache.bytes_used == 6000

    cache["too-large"] = _index("too-large", 20_000)
    assert list(cache) == ["b"]
    cache.clear()
    assert cache.bytes_used == 0


def test_jailed_jsonl_scan_uses_only_matching_claude_project(
    tmp_path: Path, monkeypatch
) -> None:
    projects = tmp_path / "projects"
    projects.mkdir()
    root_dir = "/Users/test/Desktop/Esteban"
    matching = projects / root_dir.replace("/", "-")
    unrelated = projects / "-Users-test-Downloads-Other"
    matching.mkdir()
    unrelated.mkdir()
    codex = tmp_path / "codex"
    codex.mkdir()

    monkeypatch.setattr(jsonl_sessions, "_claude_projects_dir", str(projects))
    monkeypatch.setattr(jsonl_sessions, "CODEX_SESSIONS_DIR", str(codex))
    monkeypatch.setattr(jsonl_sessions, "_root_dir", root_dir)

    assert jsonl_sessions._source_scan_roots() == [(str(matching), "claude")]


def test_claude_search_watch_roots_are_project_scoped(
    tmp_path: Path, monkeypatch
) -> None:
    projects = tmp_path / "projects"
    projects.mkdir()
    root_dir = "/Users/test/Desktop/Esteban"
    matching = projects / root_dir.replace("/", "-")
    unrelated = projects / "-Users-test-Downloads-Other"
    matching.mkdir()
    unrelated.mkdir()
    monkeypatch.setattr(claude_source, "_CLAUDE_ROOT", projects)

    source = ClaudeJsonlSource(root_dir=root_dir)

    assert source.watch_roots == [projects]
    assert source.should_index_path(matching / "session.jsonl") is True
    assert source.should_index_path(unrelated / "session.jsonl") is False
