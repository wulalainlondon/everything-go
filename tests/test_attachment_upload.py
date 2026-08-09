from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from attachment_upload import (
    BINARY_MAGIC,
    ConnectionUploadManager,
    append_video_context,
    resolve_uploaded_videos,
)


@pytest.mark.asyncio
async def test_binary_video_upload_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("attachment_upload._video_metadata", lambda _path: {"duration_ms": 14_000})
    sent: list[dict] = []

    async def send_json(payload: dict) -> None:
        sent.append(payload)

    manager = ConnectionUploadManager(
        str(tmp_path),
        {"session-1": SimpleNamespace(session_id="session-1")},
        "note20",
    )
    payload = b"\x00\x00\x00\x18ftypmp42" + b"video-data" * 100
    await manager.init(
        {
            "upload_request_id": "req-1",
            "session_id": "session-1",
            "name": "Screen Recording.mp4",
            "media_type": "video/mp4",
            "size_bytes": len(payload),
        },
        send_json,
    )
    upload_id = sent[-1]["upload_id"]
    await manager.binary(BINARY_MAGIC + upload_id.encode("ascii") + payload[:300], send_json)
    await manager.binary(BINARY_MAGIC + upload_id.encode("ascii") + payload[300:], send_json)
    await manager.finish({"upload_id": upload_id}, send_json)

    complete = sent[-1]
    assert complete["type"] == "attachment_upload_complete"
    assert complete["duration_ms"] == 14_000
    assert complete["sha256"] == hashlib.sha256(payload).hexdigest()
    remote_path = Path(complete["remote_path"])
    assert remote_path.read_bytes() == payload

    inline, videos = resolve_uploaded_videos(
        [{"name": "Screen Recording.mp4", "remote_path": str(remote_path)}],
        session_id="session-1",
        data_dir=str(tmp_path),
    )
    assert inline == []
    assert videos[0]["path"] == str(remote_path)
    context = append_video_context("Please inspect this", videos)
    assert str(remote_path) in context
    assert "duration_ms=14000" in context


@pytest.mark.asyncio
async def test_rejects_unsupported_or_oversized_video(tmp_path: Path) -> None:
    sent: list[dict] = []

    async def send_json(payload: dict) -> None:
        sent.append(payload)

    manager = ConnectionUploadManager(str(tmp_path), {"s": object()}, "note20")
    await manager.init(
        {
            "upload_request_id": "bad-type",
            "session_id": "s",
            "name": "payload.exe",
            "media_type": "application/octet-stream",
            "size_bytes": 100,
        },
        send_json,
    )
    assert sent[-1]["type"] == "attachment_upload_error"
    assert manager.active == {}


def test_resolver_rejects_path_outside_upload_store(tmp_path: Path) -> None:
    outside = tmp_path / "outside.mp4"
    outside.write_bytes(b"x")
    with pytest.raises(ValueError, match="outside"):
        resolve_uploaded_videos(
            [{"remote_path": str(outside)}],
            session_id="s",
            data_dir=str(tmp_path / "runtime"),
        )
