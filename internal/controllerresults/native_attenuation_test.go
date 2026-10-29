package controllerresults_test

import (
	"context"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/sessiondispatch"
	"testing"
	"time"
)

func TestControllerResultsNativeAttenuationPreservesOriginalAndRevocation(t *testing.T) {
	for _, stage := range []string{"no_human", "wrong_parent", "revoked_aba", "local_fence", "valid"} {
		t.Run(stage, func(t *testing.T) {
			s, scope := fixture(t)
			st := s.Results()
			ctx := context.Background()
			local := func() error { return nil }
			policy := cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 2, MaxNoProgress: 2}
			proof := cr.NativeProvenance{Source: scope.Parent, Request: "actual-parent-request", Turn: "actual-parent-turn", CallID: "actual-pause-call"}
			var original cr.Receipt
			if stage != "no_human" {
				var e error
				original, e = st.SetControl(ctx, scope, 0, policy, "human", "human-hash", local)
				if e != nil {
					t.Fatal(e)
				}
			}
			if stage == "wrong_parent" {
				proof.Source.Thread = "foreign"
			}
			if stage == "revoked_aba" {
				g, e := s.SetGrant(ctx, "parent", sessiondispatch.Grant{Enabled: false, Local: true}, scope.GrantRevision)
				if e != nil {
					t.Fatal(e)
				}
				g, e = s.SetGrant(ctx, "parent", sessiondispatch.Grant{Enabled: true, Local: true}, g.Revision)
				if e != nil {
					t.Fatal(e)
				}
				scope.GrantRevision = g.Revision
			}
			if stage == "local_fence" {
				local = func() error { return cr.ErrScope }
			}
			policy.Mode = "notify_only"
			policy.Paused = true
			r, e := st.SetControl(ctx, scope, 1, policy, "native", "native-hash", local, proof)
			if stage != "valid" {
				if e == nil {
					t.Fatal("native policy adopted", r)
				}
				if _, ok, _ := st.Operation(ctx, scope, "native", "native-hash"); ok {
					t.Fatal("denied receipt exists")
				}
				return
			}
			if e != nil || r.Control.SourceIntent != "human" || r.NativeProvenance == nil {
				t.Fatal(r, e)
			}
			again, e := st.SetControl(ctx, scope, 1, policy, "native", "native-hash", local, proof)
			if e != nil || cr.Hash(again) != cr.Hash(r) {
				t.Fatal("same intent changed", again, e)
			}
			old, ok, e := st.Operation(ctx, scope, "human", "human-hash")
			if e != nil || !ok || cr.Hash(old) != cr.Hash(original) {
				t.Fatal("human receipt rewritten", old, e)
			}
			if _, e = st.Observe(ctx, obs(scope, "after-pause", "late")); e != nil {
				t.Fatal(e)
			}
			b, _, e := st.Freeze(ctx, scope, local)
			if e != nil || b.ID != "" {
				t.Fatal("native pause admitted", b, e)
			}
		})
	}
}
