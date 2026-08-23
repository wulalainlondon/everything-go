# Release and Update Flow

The public repository is `wulalainlondon/averything-bridge`. A `v*` tag triggers `.github/workflows/release.yml`.

Required repository secrets:

- `MACOS_CERTIFICATE_P12_BASE64`
- `MACOS_CERTIFICATE_PASSWORD`
- `MACOS_KEYCHAIN_PASSWORD`
- `MACOS_CODESIGN_IDENTITY`
- `APPSTORE_CONNECT_API_KEY_ID`
- `APPSTORE_CONNECT_API_ISSUER_ID`
- `APPSTORE_CONNECT_API_KEY_P8_BASE64`

FCM service-account credentials are never release secrets and must never be
embedded in a public binary. Deployments that need push notifications pass a
local `0600` JSON file with `--service-account`.

The workflow cross-builds macOS, Linux, and Windows; packages both macOS architectures as app bundles; enforces the exact Developer ID identity; notarizes and staples; then publishes installers, binaries, app bundles, and `SHA256SUMS`.

Before tagging, follow [the release checklist](../RELEASE_CHECKLIST.md). To update a normal installation, re-run the installer from the latest release. The macOS installer verifies checksum, code signature, team ID, Gatekeeper, and notarization before it changes the service, and restores the previous signed app if the health check fails.
