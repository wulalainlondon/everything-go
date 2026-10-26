package core

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

func TestNetworkRecoveryClockFaultPausesOnlyNewDispatchAndPreservesNative(t *testing.T) {
	for _, kind := range []string{"waiting-boot", "waiting-unavailable", "accepted-boot"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
			h, fe := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			h.networkRecoveryStarted = 0
			clock := recovery.ClockAnchor{Boot: "boot", Kind: "fake-continuous", WallMS: time.Now().UnixMilli(), ElapsedMS: 1000}
			unavailable := false
			h.networkRecoveryClock = func() (recovery.ClockAnchor, error) {
				if unavailable {
					return recovery.ClockAnchor{}, recovery.ErrClock
				}
				return clock, nil
			}
			c := sharedReadClient(t, h, "clock-phone")
			s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
			done := make(chan error, 1)
			fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
				a := recovery.Automatic(ctx)
				if a == nil {
					done <- recovery.ErrGuard
					return
				}
				next, e := a.BeforeDispatch(ctx, "input", 1)
				if e != nil {
					done <- e
					return
				}
				if kind == "accepted-boot" {
					e = a.Accepted(next.Attempt.ID, "thread", "original-active")
				} else {
					_, e = a.Failed(recovery.AutomaticOutcome{Acceptance: recovery.Rejected, IngressRejected: true, Failure: recovery.Failure{Category: recovery.ModelCapacity, Source: "codex_ingress"}})
				}
				if e != nil {
					done <- e
					return
				}
				before, e := a.Resume()
				if e != nil {
					done <- e
					return
				}
				if kind == "waiting-unavailable" {
					unavailable = true
				} else {
					clock.Boot = "new-boot"
				}
				e = a.Guard()
				if !errors.Is(e, recovery.ErrClock) {
					done <- errors.New("clock fault permitted dispatch")
					return
				}
				after, e := a.Resume()
				if e != nil {
					done <- e
					return
				}
				if !after.ClockUnsafe || after.LastAttempt != before.LastAttempt || after.Attempts != before.Attempts || after.Generation != before.Generation {
					done <- errors.New("clock fault changed original attempt/generation")
					return
				}
				if kind == "accepted-boot" {
					if after.State != "accepted" || after.NativeStatus != "" {
						done <- errors.New("clock fault finished accepted native")
						return
					}
				} else if after.State != "paused_uncertain" {
					done <- errors.New("waiting clock fault not paused")
					return
				}
				done <- nil
			}
			enqueueTestMessage(t, h, c, "r_clock_fault")
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("clock observation timed out")
			}
			s.EndTurn()
		})
	}
}

func TestNetworkRecoveryUnavailableClockUsesOriginalSingleSendAndReportsNotReady(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	h.networkRecoveryClock = func() (recovery.ClockAnchor, error) { return recovery.ClockAnchor{}, recovery.ErrClock }
	c := sharedReadClient(t, h, "clock-phone")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
	done := make(chan bool, 1)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) { done <- recovery.Automatic(ctx) == nil }
	enqueueTestMessage(t, h, c, "r_clock_unavailable")
	select {
	case original := <-done:
		if !original {
			t.Fatal("unqualified recovery attached")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ordinary send blocked")
	}
	r := h.networkRecoveryReadiness()
	if r["clock_ready"] != false || r["enabled"] != false || r["durable_store_ready"] != true {
		t.Fatal(r)
	}
	if _, found, e := h.messageQueue.Recovery("s1", "r_clock_unavailable"); e != nil || found {
		t.Fatal("unqualified clock created recovery run", e)
	}
	s.EndTurn()
}

func TestNetworkRecoveryOriginalCoreActorAndLegacySnapshots(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_SESSIONS", "s1")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	c := sharedReadClient(t, h, "recovery-phone")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
	ready := make(chan recovery.AutomaticController, 1)
	finish := make(chan struct{})
	defer close(finish)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, id, _ string) {
		a := recovery.Automatic(ctx)
		if a == nil {
			ready <- nil
			return
		}
		if e := a.BindProvider("provider-subject"); e != nil {
			t.Error(e)
			ready <- nil
			return
		}
		next, e := a.BeforeDispatch(ctx, "input", 1)
		if e != nil || next.Attempt.Number != 0 {
			t.Error(e)
			ready <- nil
			return
		}
		retry, e := a.Failed(recovery.AutomaticOutcome{Acceptance: recovery.Rejected, IngressRejected: true, Failure: recovery.Failure{Category: recovery.ModelCapacity, Source: "codex_ingress"}})
		if e != nil || !retry {
			t.Error(e)
			ready <- nil
			return
		}
		ready <- a
		<-finish
	}
	enqueueTestMessage(t, h, c, "r_core_automatic")
	var a recovery.AutomaticController
	select {
	case a = <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("core never started")
	}
	if a == nil {
		t.Fatal("ordinary Send did not receive automatic coordinator")
	}
	q, found, e := h.messageQueue.Get("s1", "r_core_automatic")
	if e != nil || !found || q.State != messagequeue.Running || len(q.Payload) == 0 || s.State() != session.Streaming {
		t.Fatal("physical rejection released logical actor/input", q, s.State(), e)
	}
	snap, e := h.messageQueueSnapshot("s1")
	if e != nil || len(snap.Recovery) != 1 || snap.Items[0].State != "queued" || snap.Recovery[0].ObjectiveVerified {
		t.Fatal(snap, e)
	}
	views := h.runtimes.Snapshot("", []string{"s1"})
	if len(views) != 1 || views[0].Phase != "queued" || views[0].Stage != "queued" {
		t.Fatal("legacy runtime projection", views)
	}
	saveRecoveryFixture(t, "BACKOFF_MESSAGE_QUEUE.json", snap)
	saveRecoveryFixture(t, "BACKOFF_SESSION_RUNTIME.json", runtimeEvent(views[0]))
	if e = h.messageQueue.MarkRecoveryReconciling("s1", "r_core_automatic", "acceptance_unknown"); e != nil {
		t.Fatal(e)
	}
	h.updateRuntime("s1", "queued", "r_core_automatic", s.QueueLen(), "", "")
	h.updateRuntimeProgress("s1", "r_core_automatic", "waiting_model", "連線中斷，正在核對原任務；未重送。")
	snap, e = h.messageQueueSnapshot("s1")
	if e != nil {
		t.Fatal(e)
	}
	views = h.runtimes.Snapshot("", []string{"s1"})
	saveRecoveryFixture(t, "RECONCILING_MESSAGE_QUEUE.json", snap)
	saveRecoveryFixture(t, "RECONCILING_SESSION_RUNTIME.json", runtimeEvent(views[0]))
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"r_core_automatic"}`)
	result := waitForType(t, c, "queue_action_result")
	if result["status"] != "accepted" {
		t.Fatal("old cancel cannot cancel waiting recovery", result)
	}
	r, _ := a.Resume()
	if r.State != "cancelled" || r.Generation != 2 {
		t.Fatal(r)
	}
}
func saveRecoveryFixture(t *testing.T, name string, v any) {
	t.Helper()
	dir := os.Getenv("NETWORK_RECOVERY_FIXTURE_DIR")
	if dir == "" {
		return
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(dir, name)
	f, e := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.Write(append(raw, '\n')); e != nil {
		t.Fatal(e)
	}
}
func TestNetworkRecoveryWriterAfterRevokeNewInputAndDisable(t *testing.T) {
	for _, kind := range []string{"revoke", "new-input", "disable", "source-revision", "writer", "credential-ABA", "writer-ABA", "hidden-ABA", "model-ABA"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
			h, fe := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			h.networkRecoveryStarted = 0
			c := sharedReadClient(t, h, "owner")
			s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
			done := make(chan error, 1)
			fe.onSendContext = func(ctx context.Context, _ *session.Session, id, _ string) {
				a := recovery.Automatic(ctx)
				if a == nil {
					done <- recovery.ErrGuard
					return
				}
				if e := a.BindProvider("provider"); e != nil {
					done <- e
					return
				}
				if _, e := a.BeforeDispatch(ctx, "input", 1); e != nil {
					done <- e
					return
				}
				switch kind {
				case "credential-ABA":
					for _, b := range h.pairing.DeviceBindings() {
						h.pairing.Unclaim(b.Token)
						h.pairing.Claim(b.Token, b.DeviceID)
					}
				case "writer-ABA":
					h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
					h.controls.Reclaim(s.ID, s.ResumeID())
				case "hidden-ABA":
					yes, no := true, false
					s.SetMeta(nil, &yes)
					s.SetMeta(nil, &no)
				case "model-ABA":
					old := s.Snapshot().Model
					s.SetModel("foreign-model")
					s.SetModel(old)
				case "revoke":
					for _, b := range h.pairing.DeviceBindings() {
						h.pairing.Unclaim(b.Token)
					}
				case "new-input":
					h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s1", RequestID: "r_new", Payload: []byte(`{"content":"new"}`)})
				case "disable":
					os.WriteFile(filepath.Join(h.cfg.DataDir, "codex-network-autocontinue.disabled"), nil, 0600)
				case "source-revision":
					cfg := session.ConfigurationFrom(s.SettingsSnapshot())
					s.SetFutureConfiguration(cfg, nil)
				case "writer":
					h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
				}
				release, e := a.WriterGuard()
				if release != nil {
					release()
				}
				done <- e
			}
			enqueueTestMessage(t, h, c, "r_guard")
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("writer proceeded after", kind)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("guard never returned")
			}
			s.EndTurn()
		})
	}
}

func TestNetworkRecoveryNewInputReleasesOnlyOriginalActorPause(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("paused", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
	defer s.CloseAndWait()
	h.networkRecoveryBarriers.Store(s.ID, s.HoldQueue())
	started := make(chan struct{})
	if !s.SubmitNamed("new", func() { close(started); s.EndTurn() }) {
		t.Fatal("submit rejected")
	}
	select {
	case <-started:
		t.Fatal("paused actor ran before committed-input release")
	case <-time.After(20 * time.Millisecond):
	}
	h.releaseNetworkRecoveryBarrier(s.ID)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("invalidated recovery barrier blocked newer input")
	}
	h.releaseNetworkRecoveryBarrier(s.ID) // an idempotent release cannot underflow.
}

func TestNetworkRecoveryInitialThreadBindDoesNotResetConfigABARetirement(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	c := sharedReadClient(t, h, "qa-owner")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "")
	result := make(chan error, 1)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
		a := recovery.Automatic(ctx)
		if a == nil {
			result <- recovery.ErrGuard
			return
		}
		original := s.Snapshot().Model
		s.SetModel("foreign-model")
		s.SetModel(original) // while async initial ensureThread is outstanding
		s.SetResumeID("new-thread")
		result <- a.BindThread("new-thread")
	}
	enqueueTestMessage(t, h, c, "r_qa_bind")
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("BindThread adopted post-ABA SourceRevision and revived retired config authority")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fixture did not reach core Send")
	}
	s.EndTurn()
}

func TestNetworkRecoveryExactInitialThreadBindingAcceptsOnlySingleTransition(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	c := sharedReadClient(t, h, "owner")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "")
	result := make(chan error, 1)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
		a := recovery.Automatic(ctx)
		if a == nil {
			result <- recovery.ErrGuard
			return
		}
		s.SetResumeID("owned-thread")
		result <- a.BindThread("owned-thread")
	}
	enqueueTestMessage(t, h, c, "r_initial_bind")
	select {
	case e := <-result:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no core send")
	}
	s.EndTurn()
}
func TestNetworkRecoveryFinalWriterDoesNotWaitForCoreTransportLockInversion(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	c := sharedReadClient(t, h, "owner")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
	result := make(chan error, 1)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
		a := recovery.Automatic(ctx)
		if a == nil {
			result <- recovery.ErrGuard
			return
		}
		h.messageQueueMu.Lock()
		defer h.messageQueueMu.Unlock()
		release, e := a.WriterGuard()
		if release != nil {
			release()
		}
		result <- e
	}
	enqueueTestMessage(t, h, c, "r_lock_order")
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("busy lock should refuse final writer")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("final writer deadlocked on core lock")
	}
	s.EndTurn()
}

func TestNetworkRecoveryFinalWriterRechecksSourceAfterDurableGuardReads(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
	t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.networkRecoveryStarted = 0
	c := sharedReadClient(t, h, "qa-owner")
	s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
	result := make(chan error, 1)
	fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
		a := recovery.Automatic(ctx)
		if a == nil {
			result <- recovery.ErrGuard
			return
		}
		calls := 0
		// Deterministically inject source retirement at the time-check after the
		// final guard's durable Resume read, equivalent to mutation during that read.
		h.networkRecoveryNow = func() time.Time {
			calls++
			if calls == 2 {
				old := s.Snapshot().Model
				s.SetModel("retired-model")
				s.SetModel(old)
			}
			return time.Now()
		}
		release, e := a.WriterGuard()
		if release != nil {
			release()
		}
		result <- e
	}
	enqueueTestMessage(t, h, c, "r_qa_finalguard")
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("final core writer permit survived model ABA during its durable guard reads")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no final writer observation")
	}
	s.EndTurn()
}

func TestNetworkRecoveryFinalLocalPredicateRetiresAuthorityAfterDurableRead(t *testing.T) {
	for _, kind := range []string{"pairing-ABA", "control-ABA", "hidden-ABA", "epoch"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE", "true")
			t.Setenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY", "true")
			h, fe := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			h.networkRecoveryStarted = 0
			c := sharedReadClient(t, h, "owner")
			s := h.registry.Create("s1", "ordinary", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "thread")
			result := make(chan error, 1)
			fe.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
				a := recovery.Automatic(ctx)
				if a == nil {
					result <- recovery.ErrGuard
					return
				}
				calls := 0
				h.networkRecoveryNow = func() time.Time {
					calls++
					if calls == 2 {
						switch kind {
						case "pairing-ABA":
							for _, b := range h.pairing.DeviceBindings() {
								h.pairing.Unclaim(b.Token)
								h.pairing.Claim(b.Token, b.DeviceID)
							}
						case "control-ABA":
							h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
							h.controls.Reclaim(s.ID, s.ResumeID())
						case "hidden-ABA":
							yes, no := true, false
							s.SetMeta(nil, &yes)
							s.SetMeta(nil, &no)
						case "epoch":
							h.networkRecoveryEpoch = "retired-epoch"
						}
					}
					return time.Now()
				}
				release, e := a.WriterGuard()
				if release != nil {
					release()
				}
				result <- e
			}
			enqueueTestMessage(t, h, c, "r_final_local")
			select {
			case e := <-result:
				if e == nil {
					t.Fatal("retired final local authority permitted writer", kind)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no final writer observation")
			}
			s.EndTurn()
		})
	}
}
