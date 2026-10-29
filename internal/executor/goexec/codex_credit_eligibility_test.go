package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"everything-go/internal/recovery"
	"github.com/coder/websocket"
)

func creditQuota(allowed any, credits any) map[string]any {
	return map[string]any{"accountId": "original", "ordinaryUsageAllowed": allowed,
		"rateLimits": map[string]any{"credits": credits}}
}

func TestAutomaticCreditFailedTurnContinuation(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited"} {
		t.Run(kind, func(t *testing.T) {
			// Uses the existing real SQLite/Codex recovery fixture: original
			// accepted failure, full canonical no-effects proof, exactly one
			// continuation, immutable logical request and one terminal event.
			quota := creditQuota(false, map[string]any{"hasCredits": kind == "paid", "unlimited": kind == "unlimited"})
			testAutomaticAcceptedFailureContinuation(t, quota)
		})
	}
}

func TestAutomaticCreditQuotaFullRead(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited", "included", "exhausted", "no-credits", "upsell", "upsell-included", "spend", "reached", "reached-empty", "unknown-allowed", "unknown-even-with-credits", "missing-rate-limits", "missing-account", "malformed-credits", "epoch-change", "api-key"} {
		t.Run(kind, func(t *testing.T) {
			c, _, _, w, _ := livenessFixture(t)
			quota := creditQuota(false, map[string]any{"hasCredits": true, "unlimited": false})
			limits := quota["rateLimits"].(map[string]any)
			want := error(nil)
			switch kind {
			case "unlimited":
				limits["credits"] = map[string]any{"hasCredits": false, "unlimited": true}
			case "included":
				quota["ordinaryUsageAllowed"] = true
				delete(limits, "credits")
			case "exhausted", "no-credits":
				limits["credits"] = map[string]any{"hasCredits": false, "unlimited": false, "balance": "untrusted-positive-display"}
				if kind == "no-credits" {
					delete(limits, "credits")
				}
				want = errAutomaticUsageBlocked
			case "upsell", "upsell-included":
				quota["rateLimitUpsell"] = map[string]any{"type": "hard-denial"}
				if kind == "upsell-included" {
					quota["ordinaryUsageAllowed"] = true
				}
				want = errAutomaticUsageBlocked
			case "spend":
				limits["spendControlReached"] = true
				want = errAutomaticUsageBlocked
			case "reached", "reached-empty":
				limits["rateLimitReachedType"] = "weekly"
				if kind == "reached-empty" {
					limits["rateLimitReachedType"] = ""
				}
				want = errAutomaticUsageBlocked
			case "unknown-allowed", "unknown-even-with-credits":
				quota["ordinaryUsageAllowed"] = nil
				if kind == "unknown-allowed" {
					delete(limits, "credits")
				}
				want = recovery.ErrGuard
			case "missing-rate-limits":
				quota["ordinaryUsageAllowed"] = true
				delete(quota, "rateLimits")
				want = recovery.ErrGuard
			case "missing-account":
				delete(quota, "accountId")
				want = recovery.ErrGuard
			case "malformed-credits":
				limits["credits"] = map[string]any{"hasCredits": "true"}
				want = recovery.ErrGuard
			case "epoch-change", "api-key":
				want = recovery.ErrGuard
			}
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				switch method {
				case "account/read":
					typ := "chatgpt"
					if kind == "api-key" {
						typ = "apiKey"
					}
					return map[string]any{"account": map[string]any{"type": typ}}, nil
				case "account/rateLimits/read":
					if kind == "epoch-change" {
						c.rpc.transportEpoch.Add(1)
					}
					return quota, nil
				default:
					t.Fatal("quota check attempted effect", method)
					return nil, errors.New("unexpected effect")
				}
			}
			subject, err := c.automaticProviderSubject()
			if !errors.Is(err, want) || (want == nil && subject != recovery.Hash([]byte("chatgpt:original"))) || (want != nil && subject != "") {
				t.Fatalf("subject=%q err=%v want=%v", subject, err, want)
			}
		})
	}
}

// Real Send -> subject -> durable recovery -> startTurn -> transport; only the
// provider RPC port is synthetic. No daemon, external account or model runs.
func TestAutomaticCreditOrdinarySendAndWriterRecheck(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited", "exhausted", "late-spend-denial", "account-change"} {
		t.Run(kind, func(t *testing.T) {
			c, a, w, _, _ := automaticExecutionFixture(t)
			s := c.threadToSession["root"]
			st := c.state(s.ID)
			st.turnActive, st.turnStartPending = false, false
			c.appServerMode, c.remoteConn = "daemon", &websocket.Conn{}
			reads := 0
			w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
				switch method {
				case "account/read":
					return autoResult(map[string]any{"account": map[string]any{"type": "chatgpt"}}), nil
				case "account/rateLimits/read":
					reads++
					q := creditQuota(false, map[string]any{"hasCredits": true, "unlimited": false})
					if kind == "unlimited" {
						q = creditQuota(false, map[string]any{"hasCredits": false, "unlimited": true})
					}
					if kind == "exhausted" {
						q = creditQuota(false, nil)
					}
					if reads > 1 && kind == "late-spend-denial" {
						q["rateLimits"].(map[string]any)["spendControlReached"] = true
					}
					if reads > 1 && kind == "account-change" {
						q["accountId"] = "foreign"
					}
					return autoResult(q), nil
				case "thread/read":
					return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
				case "turn/start":
					return autoResult(map[string]any{"turn": map[string]any{"id": "credit-original-turn", "status": "completed"}}), nil
				default:
					return autoResult(map[string]any{}), nil
				}
			}
			err := c.Send(recovery.WithAutomatic(context.Background(), a), s, a.src.Request, "original objective", nil, nil)
			if kind == "exhausted" {
				if !errors.Is(err, errAutomaticUsageBlocked) {
					t.Fatal("explicit denial not retained", err)
				}
			} else {
				if err != nil {
					t.Fatal("Send rejected usable credits", err)
				}
				select {
				case <-st.turnDone:
				case <-time.After(2 * time.Second):
					t.Fatal("synthetic writer did not settle")
				}
			}
			w.mu.Lock()
			starts := len(w.starts)
			w.mu.Unlock()
			want := 0
			if kind == "paid" || kind == "unlimited" {
				want = 1
			}
			if starts != want {
				t.Fatalf("native writes=%d want=%d quota reads=%d", starts, want, reads)
			}
			if want == 1 {
				r, err := a.Resume()
				if err != nil || r.State != "native_completed" || r.Attempts != 0 {
					t.Fatal("wrong original recovery completion", r, err)
				}
			}
		})
	}
}
