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

// A full synthetic provider read with the reported included-plan/credits
// combination. Neither the display balance nor 100% establishes eligibility.
func planCreditQuota() map[string]any {
	return map[string]any{"accountId": "original", "ordinaryUsageAllowed": false, "rateLimitUpsell": nil,
		"rateLimits": map[string]any{"primary": map[string]any{"usedPercent": 100},
			"credits":             map[string]any{"hasCredits": true, "unlimited": false, "balance": "41069.82"},
			"spendControlReached": false, "rateLimitReachedType": "rate_limit_reached"}}
}

func TestCreditPlanLimitFullProviderRead(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited", "no-type-203", "credits-false", "missing-credits", "missing-has", "null-has", "missing-unlimited", "null-unlimited", "malformed-credits", "spend", "spend-missing", "spend-null", "spend-malformed", "upsell", "upsell-missing", "unknown-type", "empty-type", "workspace-owner-credit", "workspace-member-credit", "workspace-owner-limit", "workspace-member-limit", "allowed-missing", "allowed-null", "limits-missing", "limits-null", "account-missing", "epoch-change"} {
		t.Run(kind, func(t *testing.T) {
			c, _, _, w, _ := livenessFixture(t)
			q := planCreditQuota()
			limits := q["rateLimits"].(map[string]any)
			credits := limits["credits"].(map[string]any)
			var want error
			switch kind {
			case "unlimited":
				credits["hasCredits"], credits["unlimited"] = false, true
			case "no-type-203":
				delete(limits, "rateLimitReachedType")
			case "credits-false", "missing-credits":
				credits["hasCredits"] = false
				if kind == "missing-credits" {
					delete(limits, "credits")
				}
				want = errAutomaticUsageBlocked
			case "missing-has", "null-has", "missing-unlimited", "null-unlimited", "malformed-credits":
				if kind == "missing-has" {
					delete(credits, "hasCredits")
				}
				if kind == "null-has" {
					credits["hasCredits"] = nil
				}
				if kind == "missing-unlimited" {
					delete(credits, "unlimited")
				}
				if kind == "null-unlimited" {
					credits["unlimited"] = nil
				}
				if kind == "malformed-credits" {
					credits["hasCredits"] = "true"
				}
				want = recovery.ErrGuard
			case "spend":
				limits["spendControlReached"] = true
				want = errAutomaticUsageBlocked
			case "spend-missing", "spend-null", "spend-malformed":
				delete(limits, "spendControlReached")
				if kind == "spend-null" {
					limits["spendControlReached"] = nil
				}
				if kind == "spend-malformed" {
					limits["spendControlReached"] = "false"
				}
				want = recovery.ErrGuard
			case "upsell":
				q["rateLimitUpsell"] = map[string]any{"type": "provider-denial"}
				want = errAutomaticUsageBlocked
			case "upsell-missing":
				delete(q, "rateLimitUpsell")
				want = recovery.ErrGuard
			case "unknown-type", "empty-type", "workspace-owner-credit", "workspace-member-credit", "workspace-owner-limit", "workspace-member-limit":
				kinds := map[string]string{"unknown-type": "future_limit", "empty-type": "", "workspace-owner-credit": "workspace_owner_credits_depleted", "workspace-member-credit": "workspace_member_credits_depleted", "workspace-owner-limit": "workspace_owner_usage_limit_reached", "workspace-member-limit": "workspace_member_usage_limit_reached"}
				limits["rateLimitReachedType"] = kinds[kind]
				want = errAutomaticUsageBlocked
			case "allowed-missing", "allowed-null":
				delete(q, "ordinaryUsageAllowed")
				if kind == "allowed-null" {
					q["ordinaryUsageAllowed"] = nil
				}
				want = recovery.ErrGuard
			case "limits-missing", "limits-null":
				delete(q, "rateLimits")
				if kind == "limits-null" {
					q["rateLimits"] = nil
				}
				want = recovery.ErrGuard
			case "account-missing":
				delete(q, "accountId")
				want = recovery.ErrGuard
			case "epoch-change":
				want = recovery.ErrGuard
			}
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				switch method {
				case "account/read":
					return map[string]any{"account": map[string]any{"type": "chatgpt"}}, nil
				case "account/rateLimits/read":
					if kind == "epoch-change" {
						c.rpc.transportEpoch.Add(1)
					}
					return q, nil
				default:
					t.Fatal("unexpected provider effect", method)
					return nil, errors.New("forbidden")
				}
			}
			subject, err := c.automaticProviderSubject()
			if !errors.Is(err, want) || (want == nil && subject != recovery.Hash([]byte("chatgpt:original"))) || (want != nil && subject != "") {
				t.Fatalf("subject=%q err=%v want=%v", subject, err, want)
			}
		})
	}
}

func TestCreditPlanLimitAcceptedFailureOneOriginalContinuation(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited"} {
		t.Run(kind, func(t *testing.T) {
			q := planCreditQuota()
			if kind == "unlimited" {
				q["rateLimits"].(map[string]any)["credits"] = map[string]any{"hasCredits": false, "unlimited": true}
			}
			testAutomaticAcceptedFailureContinuation(t, q)
		})
	}
}

func TestCreditPlanLimitFirstSendAndFinalWriterRecheck(t *testing.T) {
	for _, kind := range []string{"paid", "unlimited", "explicit-spend", "unknown-snapshot-initial-policy", "late-spend", "late-account", "late-epoch", "late-unknown-type"} {
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
					q := planCreditQuota()
					l := q["rateLimits"].(map[string]any)
					if kind == "unlimited" {
						l["credits"] = map[string]any{"hasCredits": false, "unlimited": true}
					}
					if kind == "explicit-spend" || reads > 1 && kind == "late-spend" {
						l["spendControlReached"] = true
					}
					if kind == "unknown-snapshot-initial-policy" {
						l["spendControlReached"] = nil
					}
					if reads > 1 && kind == "late-account" {
						q["accountId"] = "foreign"
					}
					if reads > 1 && kind == "late-epoch" {
						c.rpc.transportEpoch.Add(1)
					}
					if reads > 1 && kind == "late-unknown-type" {
						l["rateLimitReachedType"] = "future_limit"
					}
					return autoResult(q), nil
				case "thread/read":
					return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
				case "turn/start":
					return autoResult(map[string]any{"turn": map[string]any{"id": "credit-original", "status": "completed"}}), nil
				default:
					return autoResult(map[string]any{}), nil
				}
			}
			err := c.Send(recovery.WithAutomatic(context.Background(), a), s, a.src.Request, "original objective", nil, nil)
			if kind == "explicit-spend" {
				if !errors.Is(err, errAutomaticUsageBlocked) {
					t.Fatal("explicit denial lost", err)
				}
			} else {
				if err != nil {
					t.Fatal("original initial request rejected", err)
				}
				select {
				case <-st.turnDone:
				case <-time.After(2 * time.Second):
					t.Fatal("writer did not settle")
				}
			}
			w.mu.Lock()
			starts := len(w.starts)
			w.mu.Unlock()
			want := 0
			if kind == "paid" || kind == "unlimited" || kind == "unknown-snapshot-initial-policy" {
				want = 1
			}
			if starts != want {
				t.Fatalf("starts=%d want=%d reads=%d", starts, want, reads)
			}
			if kind == "unknown-snapshot-initial-policy" {
				r, e := a.Resume()
				if e != nil || r.State != "disabled" {
					t.Fatal("unknown enabled automatic recovery", r, e)
				}
			}
		})
	}
}
