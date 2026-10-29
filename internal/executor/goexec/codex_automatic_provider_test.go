package goexec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/recovery"
)

func TestAutomaticProviderSubjectStableRefreshAndAccountChange(t *testing.T) {
	c, _, _, w, _ := livenessFixture(t)
	account := "original"
	allowed := true
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		switch method {
		case "account/read":
			return map[string]any{"account": map[string]any{"type": "chatgpt", "email": "ignored-refresh@example.invalid", "planType": "pro"}}, nil
		case "account/rateLimits/read":
			return map[string]any{"accountId": account, "ordinaryUsageAllowed": allowed, "rateLimits": map[string]any{}}, nil
		}
		t.Fatal("not readonly", method)
		return nil, nil
	}
	a, e := c.automaticProviderSubject()
	if e != nil {
		t.Fatal(e)
	}
	b, e := c.automaticProviderSubject()
	if e != nil || a != b || a != recovery.Hash([]byte("chatgpt:original")) {
		t.Fatal(a, b, e)
	}
	account = "other"
	b, e = c.automaticProviderSubject()
	if e != nil || b == a {
		t.Fatal("account change not detected", e)
	}
	allowed = false
	if _, e = c.automaticProviderSubject(); e != errAutomaticUsageBlocked {
		t.Fatal("blocked ordinary usage allowed", e)
	}
}
func TestAutomaticProviderSubjectMissingUnsupportedAndNullUsage(t *testing.T) {
	for _, kind := range []string{"missing", "null", "api-key", "malformed", "null-usage"} {
		t.Run(kind, func(t *testing.T) {
			c, _, _, w, _ := livenessFixture(t)
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method == "account/read" {
					typ := "chatgpt"
					if kind == "api-key" {
						typ = "apiKey"
					}
					return map[string]any{"account": map[string]string{"type": typ}}, nil
				}
				if kind == "malformed" {
					return []int{1}, nil
				}
				v := map[string]any{"rateLimits": map[string]any{}}
				if kind == "null" {
					v["accountId"] = nil
				}
				if kind == "null-usage" {
					v["accountId"] = "exact"
					v["ordinaryUsageAllowed"] = nil
				}
				return v, nil
			}
			_, e := c.automaticProviderSubject()
			if kind == "null-usage" {
				if e != recovery.ErrGuard {
					t.Fatal("unknown usage cannot authorize automatic recovery", e)
				}
			} else if e == nil {
				t.Fatal("unknown account subject accepted")
			}
		})
	}
}
func TestAutomaticProviderLiveReadOnly(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_AUTOMATIC_PROVIDER_PROBE") != "1" {
		t.Skip("explicit existing-daemon metadata-only probe")
	}
	out := os.Getenv("AUTOMATIC_PROVIDER_REPORT")
	if out == "" || !filepath.IsAbs(out) {
		t.Fatal("new absolute sanitized report path required")
	}
	c := NewCodex(&capSink{}, "codex")
	c.dataDir = t.TempDir()
	if e := c.ConnectExistingDaemon(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		c.startMu.Lock()
		defer c.startMu.Unlock()
		if e := c.stopServerLocked(); e != nil {
			t.Error(e)
		}
	}()
	subject, e := c.automaticProviderSubject()
	localSubject, localErr := c.automaticLocalSubject()
	var allowed any
	usageRaw, usageErr := c.rpcCall("account/rateLimits/read", map[string]any{}, 5*time.Second)
	if usageErr == nil {
		var usage map[string]json.RawMessage
		if json.Unmarshal(usageRaw, &usage) == nil && len(usage["ordinaryUsageAllowed"]) > 0 {
			_ = json.Unmarshal(usage["ordinaryUsageAllowed"], &allowed)
		}
	}
	reason := ""
	if e != nil {
		reason = e.Error()
	}
	result := map[string]any{"schema": 1, "at": time.Now().UTC().Format(time.RFC3339Nano), "source": "original existing daemon readonly account/read(refreshToken:false) + account/rateLimits/read", "providerSubjectPresent": subject != "", "providerSubjectSHA256": subject, "eligibleForAutomaticRecovery": e == nil && localErr == nil && subject == localSubject, "localSubjectMatchesProvider": localErr == nil && subject == localSubject, "ordinaryUsageAllowed": allowed, "reason": reason, "rawAccountOrEmailOrTokenLogged": false, "modelTurns": 0, "threadMutation": 0, "daemonRestart": 0, "credentialChange": 0}
	raw, _ := json.MarshalIndent(result, "", "  ")
	f, e := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(append(raw, '\n')); e != nil {
		f.Close()
		t.Fatal(e)
	}
	f.Close()
	t.Logf("read-only subject present=%t recoveryEligible=%t; no model/thread/daemon effects", subject != "", result["eligibleForAutomaticRecovery"])
}
