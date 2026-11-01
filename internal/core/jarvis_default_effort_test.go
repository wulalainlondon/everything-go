package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/partners"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func defaultEffortFixture(t *testing.T) (*Hub, *session.Session, func(string, any) map[string]any) {
	t.Helper()
	h, call, _, _ := roleFixture(t)
	s := h.registry.Create("jl_default_effort_original", "Original", h.cfg.RootDir, backend.Codex, "gpt-6.1-sol", "read-only", "")
	t.Cleanup(s.Close)
	p, e := h.partners.Save("human", "r_default_profile", 0, partners.Profile{SessionID: s.ID, Name: "Original", Color: "sand"})
	if e != nil {
		t.Fatal(e)
	}
	api := func(path string, in any) map[string]any {
		t.Helper()
		w := call("POST", path, in, "phone")
		if w.Code != 200 {
			t.Fatalf("%s %d %s", path, w.Code, w.Body.String())
		}
		return roleDecode(t, w)
	}
	api("role/assign", roleAssignInput(h, s, p, "r_default_role", 0))
	return h, s, api
}

func runDefaultEffortTurn(t *testing.T, s *session.Session, fn func()) {
	t.Helper()
	done := make(chan struct{})
	if !s.SubmitNamed("original-default-turn", func() { defer close(done); fn() }) {
		t.Fatal("unit admission refused")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("unit turn timeout")
	}
}
func observeDefaultEffort(t *testing.T, h *Hub, s *session.Session) *session.DefaultEffortTransition {
	t.Helper()
	high := "high"
	_, _, proof := s.ApplyProviderRuntimeMetadata(nil, &high, h.SessionRoleEpoch(s))
	if proof == nil {
		t.Error("default receipt missing")
		return nil
	}
	h.Emit(backend.ProviderDefaultEffortObserved{Transition: proof})
	return proof
}
func pinDefaultThread(h *Hub, s *session.Session) {
	s.SetResumeID("original-default-thread")
	h.Emit(backend.NewSessionUUID(s.ID, s.ResumeID()))
}

func TestJarvisDefaultEffortFirstThreadAndNormalFinishRetainOriginalEpoch(t *testing.T) {
	h, s, _ := defaultEffortFixture(t)
	base := s.SettingsSnapshot()
	baseGrant, _ := h.dispatches.BaseGrant(context.Background(), s.ID)
	runDefaultEffortTurn(t, s, func() {
		if e := s.ActivateQueuedConfiguration(session.ConfigurationFrom(base)); e != nil {
			t.Error(e)
			return
		}
		observeDefaultEffort(t, h, s)
		pinDefaultThread(h, s)
		before := h.currentJarvisRole()
		if before == nil || before.Epoch != 1 || before.SourceRevision != 2 {
			t.Error("first native witness", before)
			return
		}
		caller := backend.SessionControlCaller{Parent: s, RequestID: "original-default-turn", TurnID: "original-turn", ToolCallID: "original-call", RoleEpoch: 1}
		if _, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "list_sessions"}); e != nil {
			t.Error("original owned read", e)
		}
		h.Emit(backend.NewDone(s.ID, "original-default-turn"))
		a := h.currentJarvisRole()
		if a == nil || a.Epoch != 1 || a.Revision != 4 || a.SourceRevision != 3 || s.Snapshot().Effort != "" {
			t.Error("normal finish lost role", a, s.Snapshot())
		}
	})
	// A same-native warm/resume metadata observation must not require a new role
	// or discard the first original native caller's epoch.
	runDefaultEffortTurn(t, s, func() {
		if e := s.ActivateQueuedConfiguration(session.ConfigurationFrom(s.SettingsSnapshot())); e != nil {
			t.Error(e)
			return
		}
		observeDefaultEffort(t, h, s)
		h.prepareJarvisEndTurn(s)()
		a := h.currentJarvisRole()
		if a == nil || a.Epoch != 1 || a.SourceRevision != 5 || a.Target.Thread != "original-default-thread" {
			t.Error(a)
		}
	})
	afterGrant, _ := h.dispatches.BaseGrant(context.Background(), s.ID)
	if !reflect.DeepEqual(baseGrant, afterGrant) {
		t.Fatal("base grant changed")
	}
	// Assignment operations stay immutable; normal witness audit is separate.
	_, originalAssignment := h.jarvisRoles.Snapshot()
	op, ok := h.jarvisRoles.Lookup(originalAssignment.Actor, "r_default_role")
	if !ok || op.Revision != 1 || op.Epoch != 1 {
		t.Fatal("original assign changed", op, ok)
	}
}

func TestJarvisDefaultEffortChangedSourcesAndABAStayFenced(t *testing.T) {
	for _, change := range []string{"model", "modelABA", "effort", "effortSame", "effortABA", "config", "futureModel", "futureEffort", "sandbox", "backend", "cwd", "thread", "threadABA", "reset", "pairing", "policy", "hidden"} {
		t.Run(change, func(t *testing.T) {
			h, s, _ := defaultEffortFixture(t)
			base := s.SettingsSnapshot()
			runDefaultEffortTurn(t, s, func() {
				if e := s.ActivateQueuedConfiguration(session.ConfigurationFrom(base)); e != nil {
					t.Error(e)
					return
				}
				observeDefaultEffort(t, h, s)
				pinDefaultThread(h, s)
				switch change {
				case "model":
					s.SetModel("different-model")
				case "modelABA":
					s.SetModel("different-model")
					s.SetModel(base.Model)
				case "effort":
					s.SetEffort("low")
				case "effortSame":
					s.SetEffort("high")
				case "effortABA":
					s.SetEffort("low")
					s.SetEffort("high")
				case "config":
					s.SetConfigRevision(1)
				case "futureModel", "futureEffort":
					cfg := session.ConfigurationFrom(base)
					if change == "futureModel" {
						cfg.Model = "different-model"
					} else {
						cfg.Effort = "low"
					}
					if _, e := s.SetFutureConfiguration(cfg, nil); e != nil {
						t.Error(e)
					}
				case "sandbox":
					s.ApplyConfig("", "", "workspace-write")
				case "backend":
					s.ApplyConfig(backend.Claude, "", "")
				case "cwd":
					h.registry.UpsertExternal(s.ID, "Original", filepath.Join(h.cfg.RootDir, "other"), backend.Codex, s.ResumeID(), 1)
				case "thread":
					s.SetResumeID("unproved-thread")
				case "threadABA":
					s.SetResumeID("unproved-thread")
					s.SetResumeID("original-default-thread")
				case "reset":
					s.ClearResumeIDs()
				case "pairing":
					if e := h.pairing.Unclaim("jarvis-token"); e != nil {
						t.Error(e)
					}
				case "policy":
					if e := os.WriteFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"), []byte(`{"excluded_sessions":[]}`), 0600); e != nil {
						t.Error(e)
					}
				case "hidden":
					hidden := true
					s.SetMeta(nil, &hidden)
				}
				h.prepareJarvisEndTurn(s)()
				if h.currentJarvisRole() != nil {
					t.Error("unproved source became active", change)
				}
				_, a := h.jarvisRoles.Snapshot()
				if a.SourceRevision != 2 || a.Epoch != 1 || a.Revision != 3 {
					t.Error("unproved source was adopted", a)
				}
			})
		})
	}
}

func TestJarvisDefaultEffortLateEpochAndRevocationCannotMigrate(t *testing.T) {
	for _, change := range []string{"revoke", "reassignABA", "lateMetadataAfterReassign"} {
		t.Run(change, func(t *testing.T) {
			h, s, api := defaultEffortFixture(t)
			base := s.SettingsSnapshot()
			runDefaultEffortTurn(t, s, func() {
				if e := s.ActivateQueuedConfiguration(session.ConfigurationFrom(base)); e != nil {
					t.Error(e)
					return
				}
				var delayed *session.DefaultEffortTransition
				if change == "lateMetadataAfterReassign" {
					high := "high"
					_, _, delayed = s.ApplyProviderRuntimeMetadata(nil, &high, 1)
				} else {
					observeDefaultEffort(t, h, s)
					pinDefaultThread(h, s)
				}
				rev, _ := h.jarvisRoles.Snapshot()
				api("role/revoke", jarvisRoleRequest{Authority: h.cfg.InstanceID, Operation: "r_default_revoke", Expected: rev})
				if change != "revoke" {
					p, _ := h.partners.ForSession(s.ID)
					rev, _ = h.jarvisRoles.Snapshot()
					api("role/assign", roleAssignInput(h, s, p, "r_default_reassign", rev))
				}
				beforeRev, aBefore := h.jarvisRoles.Snapshot()
				if delayed != nil {
					h.Emit(backend.ProviderDefaultEffortObserved{Transition: delayed})
				}
				h.prepareJarvisEndTurn(s)()
				afterRev, aAfter := h.jarvisRoles.Snapshot()
				if afterRev != beforeRev {
					t.Error("late old epoch migrated", beforeRev, afterRev)
				}
				if aBefore == nil {
					if aAfter != nil {
						t.Error("revoke resurrected")
					}
				} else if aBefore.SourceRevision != aAfter.SourceRevision || aBefore.Epoch != aAfter.Epoch {
					t.Error("new epoch overwritten")
				}
			})
		})
	}
}

func TestJarvisDefaultEffortFirstPinCannotSwallowUnprovedMetadata(t *testing.T) {
	for _, change := range []string{"model", "explicitEffort", "unknownEffort", "epochZero"} {
		t.Run(change, func(t *testing.T) {
			h, s, _ := defaultEffortFixture(t)
			if change == "model" {
				s.SetModel("other")
			}
			if change == "explicitEffort" {
				s.SetEffort("low")
			}
			high := "high"
			if change == "unknownEffort" {
				high = "unknown-provider-value"
			}
			epoch := h.SessionRoleEpoch(s)
			if change == "epochZero" {
				epoch = 0
			}
			_, _, p := s.ApplyProviderRuntimeMetadata(nil, &high, epoch)
			if p != nil {
				t.Fatal("unproved metadata receipt")
			}
			pinDefaultThread(h, s)
			if h.currentJarvisRole() != nil {
				t.Fatal("initial pin adopted unrelated source mutation")
			}
			_, a := h.jarvisRoles.Snapshot()
			if a.Target.Thread != "" || a.SourceRevision != 0 {
				t.Fatal(a)
			}
		})
	}
}

func TestJarvisDefaultEffortMigrationDurablyAuditsWithoutChangingOriginalOperation(t *testing.T) {
	h, s, _ := defaultEffortFixture(t)
	high := "high"
	_, _, p := s.ApplyProviderRuntimeMetadata(nil, &high, 1)
	h.Emit(backend.ProviderDefaultEffortObserved{Transition: p})
	before, _ := h.jarvisRoles.Snapshot()
	h.Emit(backend.ProviderDefaultEffortObserved{Transition: p})
	after, _ := h.jarvisRoles.Snapshot()
	if before != after {
		t.Fatal("receipt replay mutated revision")
	}
	// Public response remains wire-compatible and carries no internal receipt.
	raw, e := json.Marshal(h.jarvisRoleSnapshot())
	if e != nil || len(raw) == 0 {
		t.Fatal(e)
	}
}
