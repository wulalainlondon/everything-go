# Averything Bridge

Averything Bridge connects the Averything mobile and desktop apps to AI coding tools running on your own computer. The backend is a single native Go service; no Python runtime or virtual environment is required.

## Supported backends

- Claude Code CLI
- Codex CLI/app-server, including shared-daemon session handoff
- Gemini CLI through ACP (`gemini --acp`)
- Ollama
- Remote WebSocket backends

The bridge provides session streaming and history, attachments, durable per-device replay, file browsing, search, artifact discovery, YouTube tasks, browser approvals, local instance management, LAN discovery, mDNS, WebRTC and an optional Cloudflare tunnel.

## Install

Install and sign in to at least one supported AI CLI first.

### macOS or Linux

```bash
curl -fsSL https://github.com/wulalainlondon/averything-bridge/releases/latest/download/install.sh | bash
```

macOS downloads a notarized app signed by `Developer ID Application: YuDi Huang (UPWLTJL6S2)`. The installer rejects a different signer, invalid Gatekeeper assessment, missing notarization ticket, or checksum mismatch. Linux verifies SHA-256 before replacing the binary.

### Windows 10/11

```powershell
irm https://github.com/wulalainlondon/averything-bridge/releases/latest/download/install.ps1 | iex
```

The PowerShell installer verifies SHA-256 and creates an `Averything Bridge` scheduled task at logon.

## Build from source

Go 1.26 or newer is required.

```bash
go test ./...
go build ./cmd/everything-go
```

For development only, run the built binary on a separate port and data directory. Production macOS deployments must use the signed and notarized release app.

```bash
./everything-go --port 8767 --data-dir /tmp/averything-bridge-dev
```

Important flags include `--claude-bin`, `--codex-bin`, `--gemini-bin`, `--ollama-host`, `--root-dir`, `--discovery`, `--mdns`, and `--tunnel`.

## Upgrade and rollback

Re-running the installer upgrades in place. On macOS it keeps `Everything Go.previous.app` until the new signed service passes its health check. The final pre-Go implementation remains available at the `python-final-20260823` tag and `legacy/python-final` branch.

## Security

- Pairing credentials are device-specific.
- Public tunnels do not start before a trusted device is paired.
- High-risk runtime operations pass through the permission gate.
- Child instance paths and ports are validated before persistence or launch.
- macOS releases are Developer ID signed and notarized; downloadable assets have SHA-256 manifests.

See [Security Policy](SECURITY.md), [Privacy Policy](docs/privacy-policy.md), and [Release Checklist](RELEASE_CHECKLIST.md).
