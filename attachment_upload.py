"""Bounded binary attachment uploads over the existing Bridge WebSocket."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable


BINARY_MAGIC = b"CBV1"
UPLOAD_ID_BYTES = 34  # ``u_`` + 32 hex characters
MAX_VIDEO_BYTES = 512 * 1024 * 1024
VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi", ".3gp", ".3gpp",
}

SendJson = Callable[[dict[str, Any]], Awaitable[Any]]


def _safe_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return (cleaned[:120] or fallback)


def _video_metadata(path: Path) -> dict[str, Any]:
    """Return best-effort ffprobe metadata without making uploads depend on it."""
    try:
        proc = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries",
                "format=duration:stream=width,height,codec_name",
                "-select_streams", "v:0", "-of", "json", str(path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if proc.returncode != 0:
            return {}
        raw = json.loads(proc.stdout or "{}")
        stream = (raw.get("streams") or [{}])[0]
        duration = float((raw.get("format") or {}).get("duration") or 0)
        result: dict[str, Any] = {}
        if duration > 0:
            result["duration_ms"] = round(duration * 1000)
        for key in ("width", "height", "codec_name"):
            if stream.get(key) is not None:
                result[key] = stream[key]
        return result
    except (OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
        return {}


@dataclass
class _Upload:
    upload_id: str
    request_id: str
    session_id: str
    device_id: str
    name: str
    media_type: str
    expected_size: int
    part_path: Path
    final_path: Path
    handle: Any
    digest: Any
    received: int = 0


class ConnectionUploadManager:
    """Owns in-progress uploads for one authenticated connection."""

    def __init__(self, data_dir: str, sessions: dict[str, Any], device_id: str):
        base = Path(data_dir or os.path.expanduser("~/.claude-bridge-runtime"))
        self.upload_root = (base / "uploads").resolve()
        self.sessions = sessions
        self.device_id = _safe_component(device_id, "unknown-device")
        self.active: dict[str, _Upload] = {}

    async def init(self, msg: dict[str, Any], send_json: SendJson) -> None:
        request_id = str(msg.get("upload_request_id") or "")
        session_id = str(msg.get("session_id") or "")
        name = str(msg.get("name") or "recording.mp4")
        media_type = str(msg.get("media_type") or "video/mp4")
        try:
            size = int(msg.get("size_bytes"))
        except (TypeError, ValueError):
            size = -1

        suffix = Path(name).suffix.lower()
        error = ""
        if not request_id:
            error = "Missing upload_request_id"
        elif session_id not in self.sessions:
            error = "Unknown session"
        elif size <= 0:
            error = "Video is empty"
        elif size > MAX_VIDEO_BYTES:
            error = "Video exceeds the 512 MB limit"
        elif suffix not in VIDEO_EXTENSIONS or not media_type.startswith("video/"):
            error = "Unsupported video format"
        if error:
            await send_json({
                "type": "attachment_upload_error",
                "upload_request_id": request_id,
                "message": error,
            })
            return

        upload_id = f"u_{uuid.uuid4().hex}"
        safe_session = _safe_component(session_id, "session")
        safe_name = _safe_component(Path(name).stem, "recording") + suffix
        target_dir = self.upload_root / safe_session / upload_id
        target_dir.mkdir(parents=True, exist_ok=False)
        part_path = target_dir / f".{safe_name}.part"
        final_path = target_dir / safe_name
        handle = part_path.open("xb")
        self.active[upload_id] = _Upload(
            upload_id=upload_id,
            request_id=request_id,
            session_id=session_id,
            device_id=self.device_id,
            name=safe_name,
            media_type=media_type,
            expected_size=size,
            part_path=part_path,
            final_path=final_path,
            handle=handle,
            digest=hashlib.sha256(),
        )
        await send_json({
            "type": "attachment_upload_ready",
            "upload_request_id": request_id,
            "upload_id": upload_id,
            "chunk_size": 256 * 1024,
        })

    async def binary(self, frame: bytes, send_json: SendJson) -> None:
        header_len = len(BINARY_MAGIC) + UPLOAD_ID_BYTES
        if len(frame) <= header_len or not frame.startswith(BINARY_MAGIC):
            return
        try:
            upload_id = frame[len(BINARY_MAGIC):header_len].decode("ascii")
        except UnicodeDecodeError:
            return
        upload = self.active.get(upload_id)
        if upload is None:
            return
        chunk = frame[header_len:]
        if upload.received + len(chunk) > upload.expected_size:
            await self._fail(upload, send_json, "Received more bytes than declared")
            return
        upload.handle.write(chunk)
        upload.digest.update(chunk)
        upload.received += len(chunk)

    async def finish(self, msg: dict[str, Any], send_json: SendJson) -> None:
        upload_id = str(msg.get("upload_id") or "")
        upload = self.active.get(upload_id)
        if upload is None:
            await send_json({
                "type": "attachment_upload_error",
                "upload_id": upload_id,
                "message": "Unknown or expired upload",
            })
            return
        if upload.received != upload.expected_size:
            await self._fail(
                upload,
                send_json,
                f"Incomplete upload ({upload.received}/{upload.expected_size} bytes)",
            )
            return
        upload.handle.flush()
        os.fsync(upload.handle.fileno())
        upload.handle.close()
        upload.part_path.replace(upload.final_path)
        metadata = _video_metadata(upload.final_path)
        manifest = {
            "version": 1,
            "upload_id": upload.upload_id,
            "session_id": upload.session_id,
            "device_id": upload.device_id,
            "name": upload.name,
            "media_type": upload.media_type,
            "size_bytes": upload.expected_size,
            "sha256": upload.digest.hexdigest(),
            "path": str(upload.final_path),
            **metadata,
        }
        manifest_path = upload.final_path.parent / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.active.pop(upload_id, None)
        await send_json({
            "type": "attachment_upload_complete",
            "upload_request_id": upload.request_id,
            **manifest,
            "remote_path": str(upload.final_path),
        })

    async def cancel(self, msg: dict[str, Any], send_json: SendJson | None = None) -> None:
        upload_id = str(msg.get("upload_id") or "")
        upload = self.active.get(upload_id)
        if upload is not None:
            self._discard(upload)

    def close(self) -> None:
        for upload in list(self.active.values()):
            self._discard(upload)

    async def _fail(self, upload: _Upload, send_json: SendJson, message: str) -> None:
        self._discard(upload)
        await send_json({
            "type": "attachment_upload_error",
            "upload_request_id": upload.request_id,
            "upload_id": upload.upload_id,
            "message": message,
        })

    def _discard(self, upload: _Upload) -> None:
        self.active.pop(upload.upload_id, None)
        try:
            upload.handle.close()
        except Exception:
            pass
        try:
            upload.part_path.unlink(missing_ok=True)
            upload.part_path.parent.rmdir()
        except OSError:
            pass


def resolve_uploaded_videos(
    files: Any,
    *,
    session_id: str,
    data_dir: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split inline files from validated uploaded video references."""
    if not isinstance(files, list):
        return [], []
    upload_root = (Path(data_dir or os.path.expanduser("~/.claude-bridge-runtime")) / "uploads").resolve()
    inline: list[dict[str, Any]] = []
    videos: list[dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict) or not item.get("remote_path"):
            inline.append(item)
            continue
        candidate = Path(str(item["remote_path"])).resolve()
        try:
            candidate.relative_to(upload_root)
        except ValueError as exc:
            raise ValueError("Uploaded video path is outside the attachment store") from exc
        manifest_path = candidate.parent / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("Uploaded video manifest is missing or invalid") from exc
        if manifest.get("session_id") != session_id or Path(str(manifest.get("path"))).resolve() != candidate:
            raise ValueError("Uploaded video does not belong to this session")
        if not candidate.is_file() or candidate.stat().st_size != int(manifest.get("size_bytes") or -1):
            raise ValueError("Uploaded video is missing or incomplete")
        videos.append(manifest)
    return inline, videos


def append_video_context(content: str, videos: list[dict[str, Any]]) -> str:
    if not videos:
        return content
    lines = [
        "",
        "",
        "[Bridge attached video files — inspect the original files at these absolute paths]",
    ]
    for video in videos:
        details = [
            f"name={video.get('name')}",
            f"path={video.get('path')}",
            f"media_type={video.get('media_type')}",
            f"size_bytes={video.get('size_bytes')}",
            f"sha256={video.get('sha256')}",
        ]
        if video.get("duration_ms"):
            details.append(f"duration_ms={video['duration_ms']}")
        if video.get("width") and video.get("height"):
            details.append(f"dimensions={video['width']}x{video['height']}")
        lines.append("- " + "; ".join(details))
    lines.append(
        "Use local video tools (for example ffprobe/ffmpeg frame extraction) to inspect them; "
        "do not claim to have reviewed the video unless you actually opened or sampled it."
    )
    return content + "\n".join(lines)
