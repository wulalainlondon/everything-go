#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <app-zip>" >&2
  exit 2
fi

ZIP_PATH="$1"
ZIP_DIR="$(dirname "$ZIP_PATH")"
ZIP_BASE="$(basename "$ZIP_PATH")"
ZIP_PATH="$(cd "$ZIP_DIR" && pwd)/$ZIP_BASE"
KEY_ID="${APPSTORE_CONNECT_API_KEY_ID:-}"
ISSUER_ID="${APPSTORE_CONNECT_API_ISSUER_ID:-}"
KEY_P8_BASE64="${APPSTORE_CONNECT_API_KEY_P8_BASE64:-}"
KEYCHAIN_PROFILE="${NOTARYTOOL_KEYCHAIN_PROFILE:-}"

if [ -z "$KEYCHAIN_PROFILE" ] && { [ -z "$KEY_ID" ] || [ -z "$ISSUER_ID" ] || [ -z "$KEY_P8_BASE64" ]; }; then
  echo "notarization failed: missing App Store Connect API key env" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

submit_args=()
if [ -n "$KEYCHAIN_PROFILE" ]; then
  submit_args=(--keychain-profile "$KEYCHAIN_PROFILE")
else
  KEY_PATH="$WORK_DIR/AuthKey_${KEY_ID}.p8"
  printf '%s' "$KEY_P8_BASE64" | base64 --decode > "$KEY_PATH" 2>/dev/null \
    || printf '%s' "$KEY_P8_BASE64" | base64 -D > "$KEY_PATH"
  submit_args=(--key "$KEY_PATH" --key-id "$KEY_ID" --issuer "$ISSUER_ID")
fi

SUBMIT_JSON="$WORK_DIR/submission.json"
xcrun notarytool submit "$ZIP_PATH" "${submit_args[@]}" --wait --output-format json | tee "$SUBMIT_JSON"
SUBMISSION_ID="$(plutil -extract id raw -o - "$SUBMIT_JSON")"
SUBMISSION_STATUS="$(plutil -extract status raw -o - "$SUBMIT_JSON")"
if [ "$SUBMISSION_STATUS" != "Accepted" ]; then
  xcrun notarytool log "$SUBMISSION_ID" "${submit_args[@]}" || true
  echo "notarization failed: Apple status $SUBMISSION_STATUS" >&2
  exit 1
fi

ditto -x -k "$ZIP_PATH" "$WORK_DIR/unzipped"
APP_PATH="$(find "$WORK_DIR/unzipped" -maxdepth 1 -name '*.app' -type d | head -n 1)"
if [ -z "$APP_PATH" ]; then
  echo "notarization failed: no .app found inside $ZIP_PATH" >&2
  exit 1
fi

xcrun stapler staple "$APP_PATH"
xcrun stapler validate "$APP_PATH"

rm -f "$ZIP_PATH"
(cd "$WORK_DIR/unzipped" && ditto -c -k --sequesterRsrc --keepParent "$(basename "$APP_PATH")" "$ZIP_PATH")
echo "notarized and stapled $ZIP_PATH"
