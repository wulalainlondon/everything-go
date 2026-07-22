from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from bridge.handlers.file_ops import handle_file_msg


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))


def ctx(root: Path) -> dict:
    return {"root_dir": str(root)}


@pytest.mark.asyncio
async def test_scan_markdown_files_lists_nested_markdown(tmp_path: Path) -> None:
    root = tmp_path / "project"
    docs = root / "docs"
    docs.mkdir(parents=True)
    (docs / "README.md").write_text("# Hello", encoding="utf-8")
    (docs / "notes.markdown").write_text("Notes", encoding="utf-8")
    (docs / "skip.txt").write_text("Nope", encoding="utf-8")

    ws = FakeWs()
    handled = await handle_file_msg(
        "scan_markdown_files",
        {"type": "scan_markdown_files", "paths": [str(root)]},
        ws,
        ctx(tmp_path),
    )

    assert handled is True
    event = ws.sent[-1]
    assert event["type"] == "markdown_files_listing"
    assert sorted(item["name"] for item in event["files"]) == ["README.md", "notes.markdown"]
    assert event["errors"] == []


@pytest.mark.asyncio
async def test_save_file_updates_markdown_atomically(tmp_path: Path) -> None:
    file_path = tmp_path / "README.md"
    file_path.write_text("# Old", encoding="utf-8")
    modified = int(file_path.stat().st_mtime)

    ws = FakeWs()
    handled = await handle_file_msg(
        "save_file",
        {
            "type": "save_file",
            "path": str(file_path),
            "content": "# New",
            "expected_modified": modified,
        },
        ws,
        ctx(tmp_path),
    )

    assert handled is True
    assert file_path.read_text(encoding="utf-8") == "# New"
    event = ws.sent[-1]
    assert event["type"] == "file_saved"
    assert event["content"] == "# New"
    assert "error" not in event


@pytest.mark.asyncio
async def test_save_file_rejects_conflicting_mtime(tmp_path: Path) -> None:
    file_path = tmp_path / "README.md"
    file_path.write_text("# Current", encoding="utf-8")

    ws = FakeWs()
    handled = await handle_file_msg(
        "save_file",
        {
            "type": "save_file",
            "path": str(file_path),
            "content": "# Stale write",
            "expected_modified": 1,
        },
        ws,
        ctx(tmp_path),
    )

    assert handled is True
    assert file_path.read_text(encoding="utf-8") == "# Current"
    assert ws.sent[-1]["error"] == "file changed on disk; reopen before saving"
