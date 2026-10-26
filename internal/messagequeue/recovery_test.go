package messagequeue

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"everything-go/internal/recovery"
)

func automaticFixture(t *testing.T) (*Store, recovery.AutomaticSource, int64) {
	t.Helper()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	source := recovery.AutomaticSource{Authority: "authority", Session: "s", Request: "r_original", Thread: "thread", Configuration: "config", Principal: "principal", Scope: "scope"}
	now := int64(1700000000000)
	if _, _, e = s.Enqueue(Entry{SessionID: source.Session, RequestID: source.Request, Payload: []byte(`{"content":"original bytes","images":[]}`)}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Transition(source.Session, source.Request, []State{Queued}, Running, "", "", ""); e != nil {
		t.Fatal(e)
	}
	if _, e = s.BeginRecovery(source, "boot1", now); e != nil {
		t.Fatal(e)
	}
	return s, source, now
}
func rejectedOutcome() recovery.AutomaticOutcome {
	return recovery.AutomaticOutcome{Failure: recovery.Failure{Category: recovery.ModelCapacity, Source: "codex_ingress"}, Acceptance: recovery.Rejected, IngressRejected: true}
}
func failedOutcome(turn string) recovery.AutomaticOutcome {
	return recovery.AutomaticOutcome{Failure: recovery.Failure{Category: recovery.ModelCapacity, Source: "codex_error_info"}, Acceptance: recovery.Accepted, Terminal: true, Thread: "thread", Turn: turn, Canonical: recovery.CanonicalEvidence{Thread: "thread", Turn: turn, Status: "failed", Complete: true, Idle: true, ToolCoverage: true, NoUnresolvedEffects: true, EvidenceHash: "canonical"}}
}
func TestAutomaticRecoveryDurableClaimAndRestartTombstone(t *testing.T) {
	s, src, now := automaticFixture(t)
	r, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 7, "input", now)
	if e != nil || a.Number != 0 || r.Attempts != 0 {
		t.Fatal(r, a, e)
	}
	if _, _, e = s.ClaimRecovery("s", src.Request, 1, "boot1", 7, "input", now); e == nil {
		t.Fatal("duplicate writer claim")
	}
	if e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	r, _, _ = s.Recovery("s", src.Request)
	if r.State != "reconciling" {
		t.Fatal(r)
	}
	q, _, _ := s.Get("s", src.Request)
	if q.State != Uncertain || len(q.Payload) == 0 {
		t.Fatal(q)
	}
	if _, _, e = s.ClaimRecovery("s", src.Request, 1, "boot2", 8, "input", now+100000); e == nil {
		t.Fatal("restart replayed unknown submission")
	}
}
func TestAutomaticRecoveryRetryWaitKeepsInputBudgetAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	src := recovery.AutomaticSource{Authority: "a", Session: "s", Request: "r", Thread: "t", Configuration: "c", Principal: "p", Scope: "scope"}
	now := time.Now().UnixMilli()
	s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{"content":"original"}`)})
	s.Transition("s", "r", []State{Queued}, Running, "", "", "")
	if _, e = s.BeginRecovery(src, "boot1", now); e != nil {
		t.Fatal(e)
	}
	s.ClaimRecovery("s", "r", 1, "boot1", 1, "input", now)
	r, ok, e := s.ScheduleRecovery("s", "r", 1, "boot1", rejectedOutcome(), now, 0)
	if e != nil || !ok {
		t.Fatal(r, e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	q, _, _ := s.Get("s", "r")
	if q.State != Queued {
		t.Fatal(q)
	}
	s.Transition("s", "r", []State{Queued}, Running, "", "", "")
	if e = s.AdoptRecovery("s", "r", 1, "boot2"); e != nil {
		t.Fatal(e)
	}
	r2, _, _ := s.Recovery("s", "r")
	if r2.Deadline != r.Deadline || r2.NextAt != r.NextAt || r2.Attempts != r.Attempts {
		t.Fatal("restart reset budget", r, r2)
	}
	if _, _, e = s.ClaimRecovery("s", "r", 1, "boot2", 2, "input", r.NextAt-1); e == nil {
		t.Fatal("early dispatch")
	}
	if _, a, e := s.ClaimRecovery("s", "r", 1, "boot2", 2, "input", r.NextAt); e != nil || a.Number != 1 {
		t.Fatal(a, e)
	}
	if data, e := s.RecoveryInput("s", "r"); e != nil || string(data) != `{"content":"original"}` {
		t.Fatal(string(data), e)
	}
	_ = filepath.Separator
}
func TestAutomaticRecoveryAcceptedTurnProofAndNoProgress(t *testing.T) {
	s, src, now := automaticFixture(t)
	_, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecoveryAccepted("s", src.Request, a.ID, "thread", "turn0", 1, "boot1", now); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		turn := []string{"turn0", "turn1", "turn2"}[i]
		r, retry, e := s.ScheduleRecovery("s", src.Request, 1, "boot1", failedOutcome(turn), now, 0)
		if e != nil {
			t.Fatal(e)
		}
		if i == 2 {
			if retry || r.State != "paused_budget" || r.NoProgress != 2 {
				t.Fatal(r, retry)
			}
			break
		}
		if !retry {
			t.Fatal(r)
		}
		now = r.NextAt
		_, a, e = s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "continuation", now)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.RecoveryAccepted("s", src.Request, a.ID, "thread", []string{"turn1", "turn2"}[i], 1, "boot1", now); e != nil {
			t.Fatal(e)
		}
	}
	proof, found, e := s.ExactNativeAcceptance("s", src.Request)
	if e != nil || !found || proof.TurnID != "turn2" {
		t.Fatal(proof, found, e)
	}
}
func TestAutomaticRecoveryCancelNewInputAndLateAckCannotRevive(t *testing.T) {
	for _, kind := range []string{"cancel", "new-input", "disable"} {
		t.Run(kind, func(t *testing.T) {
			s, src, now := automaticFixture(t)
			_, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "new-input" {
				_, _, e = s.Enqueue(Entry{SessionID: "s", RequestID: "r_new", Payload: []byte(`{"content":"new"}`)})
			} else {
				e = s.InvalidateRecovery("s", src.Request, map[string]string{"cancel": "cancelled", "disable": "disabled"}[kind])
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = s.RecoveryAccepted("s", src.Request, a.ID, "thread", "late", 1, "boot1", now); e != nil {
				t.Fatal("late receipt lost", e)
			}
			r, _, _ := s.Recovery("s", src.Request)
			if r.Generation != 2 || (r.State != "cancelled" && r.State != "disabled") {
				t.Fatal(r)
			}
			if _, _, e = s.ScheduleRecovery("s", src.Request, 1, "boot1", failedOutcome("late"), now, 0); !errors.Is(e, recovery.ErrGuard) {
				t.Fatal("late event revived cancelled run", e)
			}
		})
	}
}
func TestAutomaticRecoveryHashLeaseTransactionFailureAndCASRace(t *testing.T) {
	t.Run("hash", func(t *testing.T) {
		s, src, now := automaticFixture(t)
		s.db.Exec(`UPDATE recovery_inputs SET payload=X'7b7d'`)
		if _, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now); e == nil {
			t.Fatal("mutated input dispatched")
		}
	})
	t.Run("lease", func(t *testing.T) {
		s, src, now := automaticFixture(t)
		if _, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now+24*60*60*1000+1); e == nil {
			t.Fatal("expired input dispatched")
		}
	})
	t.Run("transaction", func(t *testing.T) {
		s, src, now := automaticFixture(t)
		s.db.Exec(`CREATE TRIGGER reject_attempt BEFORE INSERT ON recovery_attempts BEGIN SELECT RAISE(ABORT,'disk-full fixture'); END`)
		if _, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now); e == nil {
			t.Fatal("uncommitted attempt returned")
		}
		r, _, _ := s.Recovery("s", src.Request)
		if r.State != "ready" || r.LastAttempt != "" {
			t.Fatal("partial transaction", r)
		}
	})
	t.Run("race", func(t *testing.T) {
		s, src, now := automaticFixture(t)
		var wg sync.WaitGroup
		success := make(chan bool, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
				success <- e == nil
			}()
		}
		wg.Wait()
		close(success)
		n := 0
		for ok := range success {
			if ok {
				n++
			}
		}
		if n != 1 {
			t.Fatal("multiple dispatch tombstones", n)
		}
	})
}
func TestAutomaticRecoveryUnknownPermanentAndUnsupportedToolsNeverRetry(t *testing.T) {
	for _, kind := range []string{"unknown", "willRetry", "text-only", "tools", "active", "wrong-turn", "quota", "interrupted"} {
		t.Run(kind, func(t *testing.T) {
			s, src, now := automaticFixture(t)
			_, a, _ := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
			s.RecoveryAccepted("s", src.Request, a.ID, "thread", "turn", 1, "boot1", now)
			o := failedOutcome("turn")
			switch kind {
			case "unknown":
				o.Acceptance = recovery.AcceptanceUnknown
			case "willRetry":
				v := true
				o.NativeWillRetry = &v
			case "text-only":
				o.Failure.Source = "known_message"
			case "tools":
				o.Canonical.ToolCoverage = false
			case "active":
				o.Canonical.Idle = false
			case "wrong-turn":
				o.Canonical.Turn = "other"
			case "quota":
				o.Failure.Category = recovery.UsageExhausted
			case "interrupted":
				o.Canonical.Status = "interrupted"
			}
			r, retry, e := s.ScheduleRecovery("s", src.Request, 1, "boot1", o, now, 0)
			if e != nil || retry || r.State != "paused_uncertain" {
				t.Fatal(r, retry, e)
			}
		})
	}
}
func TestAutomaticRecoveryNoOldFailedBackfillAndNativeNotObjectiveVerified(t *testing.T) {
	s, src, now := automaticFixture(t)
	_, attempt, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecoveryAccepted("s", src.Request, attempt.ID, "thread", "turn", 1, "boot1", now); e != nil {
		t.Fatal(e)
	}
	if e = s.FinishRecovery("s", src.Request, "completed"); e != nil {
		t.Fatal(e)
	}
	r, _, _ := s.Recovery("s", src.Request)
	if r.State != "native_completed" || r.ObjectiveVerified {
		t.Fatal(r)
	}
	s.Enqueue(Entry{SessionID: "s", RequestID: "old", Payload: []byte(`{}`)})
	s.Transition("s", "old", []State{Queued}, Failed, "", "", "")
	src.Request = "old"
	if _, e := s.BeginRecovery(src, "boot1", now); e == nil {
		t.Fatal("old failed replay enabled")
	}
}
func TestAutomaticRecoveryScopeBreakerAndRetryAfter(t *testing.T) {
	s, src, now := automaticFixture(t)
	for i := 0; i < 3; i++ {
		_, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
		if e != nil {
			t.Fatal(e)
		}
		o := rejectedOutcome()
		if i == 0 {
			o.RetryAfter = 12 * time.Second
		}
		r, retry, e := s.ScheduleRecovery("s", src.Request, 1, "boot1", o, now, 0)
		if e != nil || !retry {
			t.Fatal(r, e)
		}
		if i == 0 && r.NextAt < now+12000 {
			t.Fatal("retry-after ignored")
		}
		now = r.NextAt
	}
	var cooldown int64
	s.db.QueryRow(`SELECT cooldown_until FROM recovery_scopes WHERE scope=?`, src.Scope).Scan(&cooldown)
	if cooldown == 0 {
		t.Fatal("breaker missing")
	}
	r, _, _ := s.Recovery("s", src.Request)
	b, _ := json.Marshal(r)
	if len(b) == 0 {
		t.Fatal("snapshot missing")
	}
}

func TestAutomaticScopeSharedAcrossDevicesAndIndependentModels(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	now := int64(1700000000000)
	runs := []recovery.AutomaticSource{}
	for i, device := range []string{"phone-a", "phone-b", "phone-c"} {
		sid := []string{"a", "b", "c"}[i]
		model := "codex:model1"
		if i == 2 {
			model = "codex:model2"
		}
		src := recovery.AutomaticSource{Authority: "authority", Session: sid, Request: "r", Thread: "thread" + sid, Principal: device, Configuration: "config", Scope: recovery.Hash([]byte(model))}
		s.Enqueue(Entry{SessionID: sid, RequestID: "r", Payload: []byte(`{"content":"original"}`)})
		s.Transition(sid, "r", []State{Queued}, Running, "", "", "")
		s.BeginRecovery(src, "boot", now)
		if e = s.BindRecoveryProvider(sid, "r", "same-provider", 1, "boot"); e != nil {
			t.Fatal(e)
		}
		r, _, _ := s.Recovery(sid, "r")
		runs = append(runs, r.Source)
		if _, _, e = s.ClaimRecovery(sid, "r", 1, "boot", 1, "input", now); e != nil {
			t.Fatal(e)
		}
		if _, _, e = s.ScheduleRecovery(sid, "r", 1, "boot", rejectedOutcome(), now, 0); e != nil {
			t.Fatal(e)
		}
	}
	if runs[0].Scope != runs[1].Scope || runs[0].Scope == runs[2].Scope {
		t.Fatal("device-specific limiter or cross-model collision", runs)
	}
	if _, _, e = s.ClaimRecovery("a", "r", 1, "boot", 1, "input", now+10000); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.ClaimRecovery("b", "r", 1, "boot", 1, "input", now+10000); e == nil {
		t.Fatal("second device bypassed half-open single writer")
	}
	if _, _, e = s.ClaimRecovery("c", "r", 1, "boot", 1, "input", now+10000); e != nil {
		t.Fatal("unrelated model blocked", e)
	}
}
func TestAutomaticNativeRetryTimeConsumesDeadlineWithoutDispatch(t *testing.T) {
	s, src, now := automaticFixture(t)
	_, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
	if e != nil {
		t.Fatal(e)
	}
	s.RecoveryAccepted("s", src.Request, a.ID, "thread", "turn", 1, "boot1", now)
	o := failedOutcome("turn")
	v := true
	o.NativeWillRetry = &v
	o.Terminal = false
	if e = s.ObserveRecoveryFailure("s", src.Request, o, now+5000); e != nil {
		t.Fatal(e)
	}
	r, _, _ := s.Recovery("s", src.Request)
	if r.FirstFailureAt != now+5000 || r.Deadline != now+1805000 || r.Attempts != 0 || r.State != "accepted" {
		t.Fatal(r)
	}
	o = failedOutcome("turn")
	r, retry, e := s.ScheduleRecovery("s", src.Request, 1, "boot1", o, now+1805000, 0)
	if e != nil || retry || r.State != "paused_budget" {
		t.Fatal("native wait reset Bridge budget", r, e)
	}
}

func TestAutomaticInputRetirementKeepsTombstonesAndUnknownAcrossRestart(t *testing.T) {
	for _, kind := range []string{"completed", "unknown", "accepted-active", "expired-unknown"} {
		t.Run(kind, func(t *testing.T) {
			s, src, now := automaticFixture(t)
			_, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "completed" || kind == "accepted-active" {
				if e = s.RecoveryAccepted("s", src.Request, a.ID, "thread", "turn", 1, "boot1", now); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "completed" {
				if e = s.FinishRecovery("s", src.Request, "completed"); e != nil {
					t.Fatal(e)
				}
				if _, _, e = s.Transition("s", src.Request, []State{Running}, Completed, "", "", ""); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "expired-unknown" {
				if e = s.Recover(); e != nil {
					t.Fatal(e)
				}
			}
			tx, e := s.db.Begin()
			if e != nil {
				t.Fatal(e)
			}
			if e = retireRecoveryInputsTx(tx); e != nil {
				t.Fatal(e)
			}
			if e = tx.Commit(); e != nil {
				t.Fatal(e)
			}
			var n int
			var hash string
			if e = s.db.QueryRow(`SELECT length(payload),hash FROM recovery_inputs WHERE session_id=? AND request_id=?`, "s", src.Request).Scan(&n, &hash); e != nil {
				t.Fatal(e)
			}
			r, _, _ := s.Recovery("s", src.Request)
			if hash != r.InputHash || (kind == "completed" && n != 0) || (kind != "completed" && n == 0) {
				t.Fatal(kind, n, r)
			}
			if kind == "completed" {
				var count int
				s.db.QueryRow(`SELECT count(*) FROM recovery_attempts WHERE session_id='s'`).Scan(&count)
				if count != 1 {
					t.Fatal("lost immutable dispatch tombstone")
				}
			}
		})
	}
}
func TestAutomaticInputPressureNeverDiscardsUnknownOrCreatesPartialRun(t *testing.T) {
	s, src, now := automaticFixture(t)
	if _, _, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now); e != nil {
		t.Fatal(e)
	}
	// A retained unknown input fills the bounded store. Expiry is not permission
	// to delete it or replay it. The new caller receives an explicit opt-out.
	if _, e := s.db.Exec(`UPDATE recovery_inputs SET payload=zeroblob(?),lease_until=?`, recovery.MaxInputBytes, now-1); e != nil {
		t.Fatal(e)
	}
	next := src
	next.Session = "new"
	next.Request = "new"
	s.Enqueue(Entry{SessionID: "new", RequestID: "new", Payload: []byte(`{"content":"new ordinary"}`)})
	s.Transition("new", "new", []State{Queued}, Running, "", "", "")
	if _, e := s.BeginRecovery(next, "boot1", now); !errors.Is(e, ErrRecoveryInputPressure) {
		t.Fatal(e)
	}
	if _, found, _ := s.Recovery("new", "new"); found {
		t.Fatal("partial run left on pressure")
	}
	q, found, e := s.Get("new", "new")
	if e != nil || !found || q.State != Running || len(q.Payload) == 0 {
		t.Fatal("ordinary admission damaged", q, e)
	}
	var n int
	s.db.QueryRow(`SELECT length(payload) FROM recovery_inputs WHERE session_id='s'`).Scan(&n)
	if n != recovery.MaxInputBytes {
		t.Fatal("unknown input deleted", n)
	}
}
func TestAutomaticCompletedWithoutDurableAcceptanceStaysUnknown(t *testing.T) {
	s, src, now := automaticFixture(t)
	s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
	if e := s.FinishRecovery("s", src.Request, "completed"); e == nil {
		t.Fatal("missing acceptance became success")
	}
	r, _, _ := s.Recovery("s", src.Request)
	if r.State != "dispatching" {
		t.Fatal(r)
	}
}

func TestAutomaticInputRetirementDoesNotClearCancelledOpaqueEffects(t *testing.T) {
	s, src, now := automaticFixture(t)
	_, a, e := s.ClaimRecovery("s", src.Request, 1, "boot1", 1, "input", now)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecoveryAccepted("s", src.Request, a.ID, "thread", "turn", 1, "boot1", now); e != nil {
		t.Fatal(e)
	}
	o := failedOutcome("turn")
	o.Canonical.ToolCoverage = false
	o.Canonical.NoUnresolvedEffects = false
	r, retry, e := s.ScheduleRecovery("s", src.Request, 1, "boot1", o, now, 0)
	if e != nil || retry || r.State != "paused_uncertain" {
		t.Fatal(r, retry, e)
	}
	if e = s.InvalidateRecovery("s", src.Request, "cancel"); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Transition("s", src.Request, []State{Running}, Cancelled, "", "", ""); e != nil {
		t.Fatal(e)
	}
	raw, e := s.RecoveryInput("s", src.Request)
	if e != nil || len(raw) == 0 {
		t.Fatal("cancel erased unresolved tool input", e)
	}
}
