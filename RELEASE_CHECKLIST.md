# Bridge Release Checklist

1. Confirm the worktree contains no unrelated or secret files: `git status --short` and a credential scan.
2. Run `go test ./...`, `go test -race ./...`, `go vet ./...`, and cross-build all release targets.
3. Validate `bash -n install.sh` and parse `install.ps1` with PowerShell when available.
4. Tag only a reviewed commit. The release job must build macOS, Linux, and Windows assets and publish `SHA256SUMS`.
5. macOS app bundles must be signed exactly by `Developer ID Application: YuDi Huang (UPWLTJL6S2)`, notarized, stapled, and accepted by Gatekeeper.
6. Before restarting Wulala or Morrie, verify the installed app with `codesign --verify --deep --strict`, inspect Authority and TeamIdentifier, run `spctl`, and validate the notarization ticket.
7. Canary Wulala first. Confirm port 8766, WebSocket connection, one isolated AI turn, per-device attachment replay, and Codex handoff. Deploy Morrie only after the canary is stable.
8. Re-run the public installer against the new GitHub release and confirm its checksum/signature gates succeed.

Never deploy or restart an unsigned, ad-hoc signed, `go run`, or temporary macOS binary.
