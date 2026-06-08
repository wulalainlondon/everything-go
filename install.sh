#!/usr/bin/env bash
#
# everything-go bridge installer
# ------------------------------------------------------------------
# One-liner (paste to your claude/codex CLI, or run in a terminal):
#
#   curl -fsSL https://github.com/wulalainlondon/everything-go/releases/latest/download/install.sh | bash
#
# What it does:
#   1. detects your OS/arch and downloads the matching static binary
#   2. makes sure `cloudflared` exists (for off-WiFi remote access)
#   3. installs everything-go as a background service (launchd / systemd)
#      started with --mdns (same-WiFi auto-discovery) and --tunnel
#      (cloudflare URL is pushed to the app so it keeps working off-WiFi)
#   4. starts it and prints how the phone app connects
#
# Prerequisite: you already have the `claude` and/or `codex` CLI installed
# and logged in. This bridge spawns *your* CLI with *your* account.
# ------------------------------------------------------------------
set -euo pipefail

REPO="${EVERYTHING_GO_REPO:-wulalainlondon/everything-go}"
PORT="${EVERYTHING_GO_PORT:-8766}"
RUNTIME_DIR="${EVERYTHING_GO_HOME:-$HOME/.everything-go-runtime}"
LABEL="com.everything-go.app"
BIN="$RUNTIME_DIR/everything-go"
APP_DIR="$RUNTIME_DIR/Everything Go.app"
APP_BIN="$APP_DIR/Contents/MacOS/everything-go"
LAUNCH="$RUNTIME_DIR/everything_go_launch.sh"
SESSION_STORE="${EVERYTHING_GO_SESSION_STORE:-$HOME/.claude-bridge-runtime/saved_sessions.json}"
SERVICE_BIN="$BIN"
PERMISSION_TARGET="$BIN"

say()  { printf '\033[1;36m[everything-go]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[everything-go]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[everything-go]\033[0m %s\n' "$*" >&2; exit 1; }

run_permission_check() {
  local extra_paths="$HOME/Downloads:$HOME/Documents:$HOME/Desktop"
  "$SERVICE_BIN" \
    --permission-check \
    --data-dir "$RUNTIME_DIR" \
    --session-store "$SESSION_STORE" \
    --permission-check-paths "$extra_paths"
}

open_full_disk_access_settings() {
  if command -v open >/dev/null 2>&1; then
    open "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles" >/dev/null 2>&1 || true
  fi
}

ensure_macos_permissions() {
  [ "$OS" = darwin ] || return 0
  [ "${EVERYTHING_GO_SKIP_PERMISSION_CHECK:-0}" = "1" ] && {
    warn "skipping macOS permission check because EVERYTHING_GO_SKIP_PERMISSION_CHECK=1"
    return 0
  }

  say "checking macOS file permissions with the installed bridge binary ..."
  if run_permission_check; then
    say "macOS permission check passed"
    return 0
  fi

  warn "macOS blocked the bridge from reading one or more local folders."
  warn "Grant Full Disk Access to: $PERMISSION_TARGET"
  warn "System Settings → Privacy & Security → Full Disk Access"
  open_full_disk_access_settings

  while true; do
    if [ ! -r /dev/tty ]; then
      die "permission approval needs an interactive terminal; re-run install.sh in Terminal after granting Full Disk Access to $SERVICE_BIN"
    fi
    printf "Press Enter after granting Full Disk Access, or Ctrl-C to stop: " >/dev/tty
    read -r _unused </dev/tty
    if run_permission_check; then
      say "macOS permission check passed"
      return 0
    fi
    warn "permission check still failed; confirm Full Disk Access is enabled for $SERVICE_BIN"
    open_full_disk_access_settings
  done
}

install_bridge_binary() {
  mkdir -p "$RUNTIME_DIR"

  # Resolve the latest tag via API to build a direct /releases/download/<tag>/
  # URL, bypassing the /releases/latest/download/ redirect which returns 504
  # for zip assets on some GitHub CDN nodes.
  local latest_tag
  latest_tag=$(curl -fsSL --proto '=https' --tlsv1.2 \
    "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
  [ -n "$latest_tag" ] || latest_tag="latest"

  base_url() { # $1 = asset filename
    if [ "$latest_tag" = "latest" ]; then
      echo "https://github.com/$REPO/releases/latest/download/$1"
    else
      echo "https://github.com/$REPO/releases/download/$latest_tag/$1"
    fi
  }

  URL=$(base_url "$ASSET")

  if [ "$OS" = darwin ]; then
    local app_asset="everything-go-darwin-${ARCH}.app.zip"
    local app_url
    app_url=$(base_url "$app_asset")
    say "downloading $app_asset ..."
    if curl -fSL --proto '=https' --tlsv1.2 "$app_url" -o "$RUNTIME_DIR/$app_asset.tmp"; then
      rm -rf "$APP_DIR.tmp" "$APP_DIR"
      ditto -x -k "$RUNTIME_DIR/$app_asset.tmp" "$RUNTIME_DIR"
      rm -f "$RUNTIME_DIR/$app_asset.tmp"
      [ -x "$APP_BIN" ] || die "app asset did not contain executable: $APP_BIN"
      SERVICE_BIN="$APP_BIN"
      PERMISSION_TARGET="$APP_DIR"
      say "app installed: $APP_DIR"
      return 0
    fi
    rm -f "$RUNTIME_DIR/$app_asset.tmp"
    warn "app asset unavailable; falling back to unsigned raw binary"
  fi

  say "downloading $ASSET ..."
  curl -fSL --proto '=https' --tlsv1.2 "$URL" -o "$BIN.tmp" \
    || die "download failed: $URL"
  chmod +x "$BIN.tmp"
  mv -f "$BIN.tmp" "$BIN"
  SERVICE_BIN="$BIN"
  PERMISSION_TARGET="$BIN"
  say "binary installed: $BIN"
}

# ── 1. platform detection ──────────────────────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=amd64 ;;
  *) die "unsupported architecture: $ARCH" ;;
esac
case "$OS" in
  darwin|linux) ;;
  *) die "unsupported OS: $OS (only macOS and Linux are supported)" ;;
esac
ASSET="everything-go-${OS}-${ARCH}"

# ── 2. prerequisite: claude or codex CLI ───────────────────────────
CLAUDE_BIN="$(command -v claude 2>/dev/null || true)"
CODEX_BIN="$(command -v codex 2>/dev/null || true)"
if [ -z "$CLAUDE_BIN" ] && [ -z "$CODEX_BIN" ]; then
  die "neither 'claude' nor 'codex' CLI found in PATH. Install and log in to at least one first."
fi
say "found CLI: ${CLAUDE_BIN:-—} ${CODEX_BIN:-—}"

# Collect the dirs that hold claude/codex so the background service's PATH
# can find them (launchd/systemd start with a minimal PATH).
CLI_PATHS=""
for b in "$CLAUDE_BIN" "$CODEX_BIN"; do
  [ -n "$b" ] && CLI_PATHS="$CLI_PATHS:$(dirname "$b")"
done

# ── 3. download the bridge binary/app ──────────────────────────────
install_bridge_binary

# ── 4. ensure cloudflared (remote access; optional but recommended) ─
CF_PATH=""
if command -v cloudflared >/dev/null 2>&1; then
  CF_PATH="$(dirname "$(command -v cloudflared)")"
elif [ "$OS" = darwin ] && command -v brew >/dev/null 2>&1; then
  say "installing cloudflared via Homebrew ..."
  brew install cloudflared && CF_PATH="$(dirname "$(command -v cloudflared)")"
else
  # Linux ships a raw binary; macOS only ships .tgz/.pkg, so non-brew mac
  # users get a warning instead.
  if [ "$OS" = linux ]; then
    say "downloading cloudflared ..."
    curl -fSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}" \
      -o "$RUNTIME_DIR/cloudflared" && chmod +x "$RUNTIME_DIR/cloudflared" \
      && CF_PATH="$RUNTIME_DIR"
  fi
fi
if [ -z "$CF_PATH" ]; then
  warn "cloudflared not available — same-WiFi will still work via mDNS, but"
  warn "off-WiFi remote access needs cloudflared (install Homebrew then re-run)."
fi

# ── 5. generate the launch wrapper ─────────────────────────────────
# Service-managers start with a bare PATH; bake in the CLI + cloudflared dirs.
SERVICE_PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin${CLI_PATHS}${CF_PATH:+:$CF_PATH}"
cat > "$LAUNCH" <<EOF
#!/usr/bin/env bash
set -uo pipefail
export PATH="$SERVICE_PATH:\$PATH"
exec "$SERVICE_BIN" \\
  --port "$PORT" \\
  --executor go \\
  --data-dir "$RUNTIME_DIR" \\
  --session-store "$SESSION_STORE" \\
  --mdns \\
  --tunnel \\
  --instance-name "\$(hostname -s 2>/dev/null || echo everything-go)"
EOF
chmod +x "$LAUNCH"
say "launch script written: $LAUNCH"
ensure_macos_permissions

# ── 6. install as a background service ─────────────────────────────
install_launchd() {
  local plist="$HOME/Library/LaunchAgents/$LABEL.plist"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string><string>-lc</string><string>exec $LAUNCH</string>
  </array>
  <key>WorkingDirectory</key><string>$RUNTIME_DIR</string>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>/tmp/$LABEL.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/$LABEL.stderr.log</string>
</dict>
</plist>
EOF
  local target="gui/$(id -u)/$LABEL"
  if launchctl print "$target" >/dev/null 2>&1; then
    launchctl kickstart -k "$target"
  else
    launchctl bootstrap "gui/$(id -u)" "$plist"
    launchctl kickstart -k "$target"
  fi
}

install_systemd() {
  local unit_dir="$HOME/.config/systemd/user"
  mkdir -p "$unit_dir"
  cat > "$unit_dir/everything-go.service" <<EOF
[Unit]
Description=everything-go bridge
After=network-online.target

[Service]
ExecStart=$LAUNCH
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now everything-go.service
  command -v loginctl >/dev/null 2>&1 && loginctl enable-linger "$(whoami)" 2>/dev/null || true
}

if [ "$OS" = darwin ]; then
  install_launchd
else
  if command -v systemctl >/dev/null 2>&1; then
    install_systemd
  else
    warn "no systemd — starting in foreground (will stop when you close this shell)."
    exec "$LAUNCH"
  fi
fi

# ── 7. report ──────────────────────────────────────────────────────
LAN_IP="$( (ipconfig getifaddr en0 2>/dev/null) || (hostname -I 2>/dev/null | awk '{print $1}') || true )"
echo
say "✅ everything-go is running on port $PORT"
echo
say "📱 In the phone app:"
say "   • Same WiFi: it auto-discovers this bridge (mDNS). Just open the app."
[ -n "$LAN_IP" ] && say "     (manual fallback — add bridge IP: $LAN_IP  port: $PORT)"
say "   • Off WiFi: connect once on WiFi; the cloudflare URL is pushed to the"
say "     app automatically, so it keeps working after you leave."
echo
say "logs:    /tmp/$LABEL.stderr.log"
say "restart: launchctl kickstart -k gui/\$(id -u)/$LABEL   (macOS)"
