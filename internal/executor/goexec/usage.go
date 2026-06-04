package goexec

import (
	"context"
	"encoding/json"
	"os/exec"
	"time"

	"everything-go/internal/protocol"
)

// bunUsageScript reads the Claude Code OAuth token from the macOS keychain and
// queries the claude.ai usage endpoint. Byte-for-byte the Python bridge's
// _BUN_USAGE_SCRIPT (backends/claude_cli.py fetch_usage).
const bunUsageScript = `
const { execSync } = require('child_process');
const raw = execSync("security find-generic-password -s 'Claude Code-credentials' -g 2>&1").toString();
const creds = JSON.parse(raw.match(/password: "(.+)"/)[1]);
const token = creds.claudeAiOauth.accessToken;
const res = await fetch('https://claude.ai/api/oauth/usage', {
  headers: { 'Authorization': ` + "`Bearer ${token}`" + ` }
});
const data = await res.json();
console.log(JSON.stringify(data));
`

// FetchUsage runs the bun helper and maps the claude.ai usage payload to a
// usage_report. Returns nil (no error) if usage can't be fetched, so the caller
// can simply skip emitting for this backend.
func (c *Claude) FetchUsage(ctx context.Context) (*protocol.UsageReport, error) {
	bun := "bun"
	if p, err := exec.LookPath("bun"); err == nil {
		bun = p
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(cctx, bun, "-e", bunUsageScript).Output()
	if err != nil {
		return nil, nil
	}
	var data struct {
		FiveHour       *claudeUsageEntry `json:"five_hour"`
		SevenDay       *claudeUsageEntry `json:"seven_day"`
		SevenDaySonnet *claudeUsageEntry `json:"seven_day_sonnet"`
	}
	if err := json.Unmarshal(out, &data); err != nil {
		return nil, nil
	}
	rep := protocol.NewUsageReport(
		data.FiveHour.window(),
		data.SevenDay.window(),
		data.SevenDaySonnet.window(),
	)
	return &rep, nil
}

type claudeUsageEntry struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    *string  `json:"resets_at"`
}

// window converts a claude.ai entry to a wire UsageWindow: utilization is a
// 0..100 percent there, normalized to a 0..1 fraction here (matching Python).
func (e *claudeUsageEntry) window() *protocol.UsageWindow {
	if e == nil {
		return nil
	}
	var util *float64
	if e.Utilization != nil {
		f := *e.Utilization / 100.0
		util = &f
	}
	return &protocol.UsageWindow{Utilization: util, ResetsAt: e.ResetsAt}
}

// FetchUsage queries the Codex app-server rate limits and maps primary/secondary
// windows to five_hour/seven_day. Mirrors codex_appserver.py fetch_usage.
func (c *Codex) FetchUsage(ctx context.Context) (*protocol.UsageReport, error) {
	if err := c.ensureServer(); err != nil {
		return nil, nil
	}
	raw, err := c.rpcCall("account/rateLimits/read", nil, 10*time.Second)
	if err != nil {
		return nil, nil
	}
	var res struct {
		RateLimits  map[string]json.RawMessage `json:"rateLimits"`
		RateLimits2 map[string]json.RawMessage `json:"rate_limits"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, nil
	}
	limits := res.RateLimits
	if limits == nil {
		limits = res.RateLimits2
	}
	five := codexWindow(limits["primary"])
	seven := codexWindow(limits["secondary"])
	rep := protocol.NewUsageReport(five, seven, nil)
	return &rep, nil
}

// codexWindow maps a Codex rate-limit window. usedPercent is 0..100 → 0..1;
// resetsAt may be an epoch seconds number (converted to ISO) or a string.
func codexWindow(raw json.RawMessage) *protocol.UsageWindow {
	if len(raw) == 0 {
		return nil
	}
	var w struct {
		UsedPercent  *float64        `json:"usedPercent"`
		UsedPercent2 *float64        `json:"used_percent"`
		ResetsAt     json.RawMessage `json:"resetsAt"`
		ResetsAt2    json.RawMessage `json:"resets_at"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil
	}
	var util *float64
	pct := w.UsedPercent
	if pct == nil {
		pct = w.UsedPercent2
	}
	if pct != nil {
		f := *pct / 100.0
		util = &f
	}
	resets := w.ResetsAt
	if len(resets) == 0 {
		resets = w.ResetsAt2
	}
	return &protocol.UsageWindow{Utilization: util, ResetsAt: resetsAtString(resets)}
}

// resetsAtString accepts a JSON value that is either a string or an epoch
// seconds number, returning an ISO-8601 UTC string for the numeric case.
func resetsAtString(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return &s
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		if f > 1e9 {
			iso := time.Unix(int64(f), 0).UTC().Format(time.RFC3339)
			return &iso
		}
		iso := time.Unix(int64(f), 0).UTC().Format(time.RFC3339)
		return &iso
	}
	return nil
}
