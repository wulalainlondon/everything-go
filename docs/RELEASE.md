# Release & Update Flow

## Prerequisites

| Item | Value |
|------|-------|
| Repo | `wulalainlondon/everything-go` |
| Developer ID Team | `UPWLTJL6S2` |
| CI trigger | push tag matching `v*` |
| CI file | `.github/workflows/release.yml` |

All 8 GitHub Secrets are pre-configured — do not modify them:
`MACOS_CERTIFICATE_P12_BASE64`, `MACOS_CERTIFICATE_PASSWORD`, `MACOS_KEYCHAIN_PASSWORD`,
`MACOS_CODESIGN_IDENTITY`, `APPSTORE_CONNECT_API_KEY_ID`, `APPSTORE_CONNECT_API_ISSUER_ID`,
`APPSTORE_CONNECT_API_KEY_P8_BASE64`, `FCM_SERVICE_ACCOUNT_JSON`

---

## Releasing a new version

### 1. Commit and push code changes

```bash
git add .
git commit -m "feat: ..."
git push
```

### 2. Push a version tag

```bash
git tag v0.1.2
git push origin v0.1.2
```

The CI pipeline runs automatically (~3–5 min) and:
1. Cross-compiles for `darwin/arm64`, `darwin/amd64`, `linux/arm64`, `linux/amd64`
2. Signs the macOS `.app` bundle with the Developer ID certificate
3. Submits to Apple for notarization (`notarytool submit --wait`)
4. Staples the notarization ticket back into the `.app.zip`
5. Publishes all assets to GitHub Releases with a `SHA256SUMS` file

> **Tag format is required.** The tag must match `v*` (e.g. `v0.1.2`).
> A plain commit push does not trigger the release workflow.

---

## Updating the local installation

```bash
EVERYTHING_GO_SKIP_PERMISSION_CHECK=1 \
  bash ~/Downloads/Helper/claude-bridge/go/install.sh
```

`install.sh` will:
- Download the new `everything-go-darwin-arm64.app.zip` from the latest release
- Replace `~/.everything-go-runtime/Everything Go.app/`
- Restart the launchd service (`com.everything-go.app`)

### Verify the update

```bash
# Service running (exit code must be 0)
launchctl list | grep everything-go

# Port 8766 listening
lsof -Pan -p $(pgrep -x everything-go) -i | grep LISTEN

# Correct Developer ID (not adhoc)
codesign -d --verbose=4 \
  ~/.everything-go-runtime/Everything\ Go.app/Contents/MacOS/everything-go 2>&1 \
  | grep TeamIdentifier
# Expected: TeamIdentifier=UPWLTJL6S2
```

---

## What does NOT need to change between releases

- `~/Library/LaunchAgents/com.everything-go.app.plist`
- `~/.everything-go-runtime/everything_go_launch.sh`
- Any GitHub Secret

---

## Troubleshooting

### Service shows exit `-9` (SIGKILL) after update

The `.app` bundle binary has a broken codesign. Fix:

```bash
rm -rf /tmp/eg-fresh.app
mkdir -p /tmp/eg-fresh.app/Contents/{MacOS,Resources}
cp ~/.everything-go-runtime/Everything\ Go.app/Contents/Info.plist \
   /tmp/eg-fresh.app/Contents/
cp ~/.everything-go-runtime/Everything\ Go.app/Contents/MacOS/everything-go \
   /tmp/eg-fresh.app/Contents/MacOS/
codesign --force --deep --sign - /tmp/eg-fresh.app
mv ~/.everything-go-runtime/Everything\ Go.app \
   ~/.everything-go-runtime/Everything\ Go.app.bak
ditto /tmp/eg-fresh.app ~/.everything-go-runtime/Everything\ Go.app
launchctl kickstart -k gui/$(id -u)/com.everything-go.app
```

Root cause: macOS sets `com.apple.macl` on downloaded files, which prevents
`codesign --force` from replacing the signature in-place. Building a fresh bundle
in `/tmp` (no xattrs) and replacing via `ditto` works around this.

This should not happen with properly notarized releases, but can occur if the
release was built without valid signing secrets (ad-hoc fallback).
