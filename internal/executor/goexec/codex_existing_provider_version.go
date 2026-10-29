package goexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// initialize.userAgent's leading version is the running server's build version,
// not clientInfo.version in its trailing suffix. Unknown shapes stay unqualified.
// Source: openai/codex initialize_processor.rs and auth/default_client.rs.
var existingProviderAgentVersion = regexp.MustCompile(`^[A-Za-z0-9_. -]{1,80}/([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?) \(`)

func existingProviderDiagnostics(raw json.RawMessage, epoch uint64, socket string) map[string]any {
	out := map[string]any{"status": "externally_managed", "connection_only": true, "socket_path": socket, "checked_at_ms": time.Now().UnixMilli(), "running_version": "unknown", "version_source": "initialize.userAgent", "version_transport_epoch": epoch, "version_observed": false}
	var response struct {
		UserAgent string `json:"userAgent"`
	}
	if len(raw) > 65536 || json.Unmarshal(raw, &response) != nil || len(response.UserAgent) > 4096 || strings.ContainsAny(response.UserAgent, "\r\n\x00") {
		return out
	}
	match := existingProviderAgentVersion.FindStringSubmatch(response.UserAgent)
	if len(match) != 2 {
		return out
	}
	out["running_version"] = match[1]
	out["version_observed"] = true
	digest := sha256.Sum256([]byte(response.UserAgent))
	out["initialize_user_agent_sha256"] = hex.EncodeToString(digest[:])
	return out
}

func (c *Codex) refreshExistingProviderDiagnostics() {
	epoch := c.rpc.transportEpoch.Load()
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	old, _ := c.runtimeDiagnostics["codex"].(map[string]any)
	// Refresh must not discard a genuine handshake from the same live transport,
	// nor retain a version from a retired writer after disconnect/reconnect.
	out := existingProviderDiagnostics(nil, epoch, c.appServerSocket)
	oldEpoch, ok := old["version_transport_epoch"].(uint64)
	if ok && oldEpoch == epoch && old["version_source"] == "initialize.userAgent" && old["version_observed"] == true {
		for _, key := range []string{"running_version", "version_observed", "initialize_user_agent_sha256"} {
			out[key] = old[key]
		}
	}
	c.runtimeDiagnostics = map[string]any{"codex": out}
}
