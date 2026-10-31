package goexec

import (
	"encoding/json"
	"everything-go/internal/recovery"
	"testing"
)

func TestProviderPreflightRetriesOnlyMetadataAndKeepsEpoch(t *testing.T) {
	for _, mode := range []string{"transient", "blocked", "epoch", "persistent"} {
		t.Run(mode, func(t *testing.T) {
			c, _, _, w, _ := livenessFixture(t)
			calls := 0
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method != "account/read" && method != "account/rateLimits/read" {
					t.Fatal("non-read RPC", method)
				}
				if method == "account/read" {
					calls++
					if mode == "epoch" {
						c.rpc.transportEpoch.Add(1)
					}
					if mode == "persistent" || (mode == "transient" && calls == 1) {
						return map[string]any{"account": nil}, nil
					}
					return map[string]any{"account": map[string]string{"type": "chatgpt"}}, nil
				}
				return map[string]any{"accountId": "original", "ordinaryUsageAllowed": mode != "blocked", "rateLimits": map[string]any{}}, nil
			}
			epoch := c.rpc.transportEpoch.Load()
			_, e := c.providerSubjectBeforeDispatch(epoch)
			if mode == "transient" {
				if e != nil || calls != 2 {
					t.Fatal(e, calls)
				}
			} else {
				if e == nil {
					t.Fatal("invalid check passed")
				}
				want := 1
				if mode == "persistent" {
					want = 2
				}
				if calls != want {
					t.Fatal(calls, want)
				}
			}
		})
	}
}
func TestProviderPreDispatchIsNotUnknownOrIngressRejected(t *testing.T) {
	e := &codexAcceptanceError{Reason: "native_provider_subject_unavailable", PreDispatch: true, Failure: recovery.Failure{Category: recovery.ConfigurationError}}
	d := codexSubmissionFailure(e)
	if d.Acceptance != recovery.Rejected || d.IngressRejected {
		t.Fatal(d)
	}
	e.PreDispatch = false
	d = codexSubmissionFailure(e)
	if d.Acceptance != recovery.AcceptanceUnknown {
		t.Fatal("post-submit uncertainty weakened")
	}
}
