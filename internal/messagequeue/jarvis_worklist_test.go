package messagequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestUnifiedWorkItemsDurableCASAndOriginalIntent(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	in := WorkMutation{IntentID: "r_record_original", WorkItem: WorkItem{ID: "note", Kind: "discussion_note", Title: "只記錄，不派工", RecordingState: "recorded"}}
	a, e := s.MutateWork(ctx, "phone\x001", "manager", in)
	if e != nil || a.Revision != 1 {
		t.Fatal(a, e)
	}
	same, e := s.MutateWork(ctx, "phone\x001", "manager", in)
	if e != nil || same.Revision != 1 {
		t.Fatal(same, e)
	}
	in.Title = "changed"
	if _, e = s.MutateWork(ctx, "phone\x001", "manager", in); !errors.Is(e, ErrWorkConflict) {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if v, e := s.WorkOperation(ctx, "phone\x001", "manager", "r_record_original"); e != nil || v.Title != a.Title {
		t.Fatal(v, e)
	}
	if _, e := s.WorkOperation(ctx, "phone\x002", "manager", "r_record_original"); e == nil {
		t.Fatal("credential epoch leak")
	}
	items, _, e := s.WorkItems(ctx, "phone\x001", "manager", "", 30)
	if e != nil || len(items) != 1 {
		t.Fatal(items, e)
	}
	snap, e := s.Snapshot("manager")
	if e != nil || len(snap.Items) != 0 {
		t.Fatal("note dispatched", snap, e)
	}
	in.IntentID = "r_second_mutation"
	in.ExpectedRevision = 1
	in.Links = []WorkLink{{Kind: "intake", RequestID: "r_work"}}
	a, e = s.MutateWork(ctx, "phone\x001", "manager", in)
	if e != nil {
		t.Fatal(e)
	}
	in.IntentID = "r_third_mutation"
	in.ExpectedRevision = a.Revision
	in.Links = nil
	if _, e = s.MutateWork(ctx, "phone\x001", "manager", in); e == nil {
		t.Fatal("historical link lost")
	}
}
func TestUnifiedWorkPlanIsNotCapturedNoteOrAuthorization(t *testing.T) {
	s, _ := Open("")
	defer s.Close()
	in := WorkMutation{IntentID: "r_record_original", WorkItem: WorkItem{ID: "p", Kind: "planning", Title: "plan", RecordingState: "plan_ready"}}
	if _, e := s.MutateWork(context.Background(), "scope", "s", in); e == nil {
		t.Fatal("missing plan accepted")
	}
	in.Plan = &WorkPlan{Path: "plan.txt", SHA256: string(make([]byte, 64)), Revision: 1, Checklist: []string{"a", "a", "b", "c", "d", "e"}}
	if _, e := s.MutateWork(context.Background(), "scope", "s", in); e == nil {
		t.Fatal("duplicate checklist accepted")
	}
	in.Plan.Checklist = []string{"scope", "sources", "interaction", "data", "dependencies", "acceptance"}
	in.Decision = &WorkDecision{State: "approved"}
	if _, e := s.MutateWork(context.Background(), "scope", "s", in); e == nil {
		t.Fatal("approval has no human source")
	}
}
func liveStart(t *testing.T, s *Store, id string) NativeStart {
	t.Helper()
	_, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: id, Payload: []byte(`{}`)})
	if e != nil {
		t.Fatal(e)
	}
	_, _, e = s.Transition("s", id, []State{Queued}, Running, "", "", "")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecordNativeAcceptance("s", id, "thread", "turn"); e != nil {
		t.Fatal(e)
	}
	return NativeStart{SessionID: "s", RequestID: id, Provider: "codex", ThreadID: "thread", Token: "turn", TokenKind: "turn_id", ProofKind: "live_turn_start_in_progress", ObservedAt: time.Now().UnixMilli()}
}
func TestUnifiedNativeStartNotRuntimeAcceptanceOrTerminal(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	p := liveStart(t, s, "r_live")
	if _, ok, e := s.NativeStart("s", "r_live"); e != nil || ok {
		t.Fatal("acceptance invented start", e)
	}
	bad := p
	bad.ProofKind = "runtime_running"
	if _, e := s.RecordNativeStart(bad); e == nil {
		t.Fatal("generic runtime proof")
	}
	bad = p
	bad.Token = "foreign"
	if _, e := s.RecordNativeStart(bad); e == nil {
		t.Fatal("foreign tuple")
	}
	ok, e := s.RecordNativeStart(p)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	ok, e = s.RecordNativeStart(p)
	if e != nil || ok {
		t.Fatal("duplicate logical start", ok, e)
	}
	s.Close()
	s, _ = Open(dir)
	defer s.Close()
	got, ok, e := s.NativeStart("s", "r_live")
	if e != nil || !ok || got.Token != "turn" {
		t.Fatal(got, ok, e)
	}
	p2 := liveStart(t, s, "r_round2")
	ok, e = s.RecordNativeStart(p2)
	if e != nil || !ok {
		t.Fatal("second user round lost", e)
	}
	s.Transition("s", "r_round2", []State{Running}, Completed, "", "", "")
	if _, e = s.RecordNativeStart(p2); e == nil {
		t.Fatal("late terminal emitted start")
	}
	notices, e := s.WorkNotices(context.Background(), "scope", "manager", []WorkLink{{SessionID: "s", RequestID: "r_live"}, {SessionID: "s", RequestID: "r_live"}})
	if e != nil || len(notices) != 1 || notices[0].Receipts["displayed"] != "unknown" {
		t.Fatal(notices, e)
	}
}
func TestUnifiedClaudeStartNeverManufacturesCodexTurn(t *testing.T) {
	s, _ := Open("")
	defer s.Close()
	p := liveStart(t, s, "r_claude")
	p.Provider = "claude"
	p.TokenKind = "message_uuid"
	p.Token = "input-uuid"
	p.ProofKind = "live_assistant_after_input_ack"
	if _, e := s.RecordNativeStart(p); e == nil {
		t.Fatal("no input ACK")
	}
	if e := s.RecordProviderEvidence(ProviderEvidence{SessionID: "s", RequestID: p.RequestID, Backend: "claude", ConversationID: "thread", Token: "input-uuid", TokenKind: "message_uuid"}); e != nil {
		t.Fatal(e)
	}
	if ok, e := s.RecordNativeStart(p); e != nil || !ok {
		t.Fatal(ok, e)
	}
}
func TestUnifiedResetArchiveReadbackBarrierAndUnknown(t *testing.T) {
	ctx := context.Background()
	s, _ := Open(t.TempDir())
	defer s.Close()
	archive := json.RawMessage(`{"messages":[{"content":"原正文"}],"complete":true}`)
	in := ContextReset{IntentID: "r_reset_original", SessionID: "s", ThreadID: "old", SourceRevision: 4, Mode: "direct", ArchiveHash: workHash(archive)}
	v, e := s.SaveContextReset(ctx, "scope", "hash", in, archive)
	if e != nil || v.Phase != "prepared" {
		t.Fatal(v, e)
	}
	read, hash, e := s.ContextArchive(ctx, "scope", v.IntentID)
	if e != nil || string(read) != string(archive) || hash != in.ArchiveHash {
		t.Fatal(string(read), hash, e)
	}
	if _, _, e = s.ContextArchive(ctx, "other", v.IntentID); e == nil {
		t.Fatal("archive scope leak")
	}
	v, e = s.ReserveContextReset(ctx, "scope", v.IntentID, v.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Enqueue(Entry{SessionID: "s", RequestID: "new", Payload: []byte(`{}`)}); e == nil {
		t.Fatal("input raced generation")
	}
	v, e = s.ContextResetPhase(ctx, "scope", v.IntentID, "reserved", "reset_issued", "")
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.ContextResetPhase(ctx, "scope", v.IntentID, "reset_issued", "unknown", "timeout")
	if e != nil || v.Phase != "unknown" {
		t.Fatal(v, e)
	}
	if active, e := s.ContextBarrier(ctx, "s"); e != nil || !active {
		t.Fatal("unknown released barrier", active, e)
	}
	if _, e = s.ReserveContextReset(ctx, "scope", v.IntentID, v.Revision); e == nil {
		t.Fatal("unknown repeated reset")
	}
}
func TestUnifiedResetQueuedAndUncertainBlockBeforeEffect(t *testing.T) {
	for _, state := range []State{Queued, Running, Uncertain, Steering} {
		t.Run(string(state), func(t *testing.T) {
			s, _ := Open("")
			defer s.Close()
			_, _, _ = s.Enqueue(Entry{SessionID: "s", RequestID: "pending", Payload: []byte(`{}`)})
			if state != Queued {
				s.Transition("s", "pending", []State{Queued}, state, "", "", "")
			}
			raw := json.RawMessage(`{}`)
			v, e := s.SaveContextReset(context.Background(), "p", "h", ContextReset{IntentID: "r_reset_original", SessionID: "s", ArchiveHash: workHash(raw)}, raw)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.ReserveContextReset(context.Background(), "p", v.IntentID, v.Revision); e == nil {
				t.Fatal("unsettled input permitted reset")
			}
		})
	}
}

func TestUnifiedStartPushOutboxPolicyCASAndNoUnknownReplay(t *testing.T) {
	ctx := context.Background()
	s, _ := Open(t.TempDir())
	defer s.Close()
	sub := NoticeSubscriber{Scope: "scope", Manager: "m", Input: json.RawMessage(`{"enabled":true}`), Enabled: true}
	r, e := s.SetNoticePolicy(ctx, sub, "r_subscribe", 0)
	if e != nil || r.Revision != 1 {
		t.Fatal(r, e)
	}
	r, e = s.SetNoticePolicy(ctx, sub, "r_subscribe", 0)
	if e != nil || r.Revision != 1 {
		t.Fatal(r, e)
	}
	if _, e = s.SetNoticePolicy(ctx, sub, "r_other", 0); e == nil {
		t.Fatal("policy CAS overwritten")
	}
	p := StartPush{Scope: "scope", Manager: "m", EventID: "start_exact", Payload: json.RawMessage(`{}`), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if e = s.PrepareStartPush(ctx, p); e != nil {
		t.Fatal(e)
	}
	if ok, e := s.ClaimStartPush(ctx, p); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if ok, e := s.ClaimStartPush(ctx, p); e != nil || ok {
		t.Fatal("issued operation replayed", ok, e)
	}
	if e = s.FinishStartPush(ctx, p, "unknown", json.RawMessage(`{"state":"unknown"}`)); e != nil {
		t.Fatal(e)
	}
	s.PrepareStartPush(ctx, p)
	if ok, e := s.ClaimStartPush(ctx, p); e != nil || ok {
		t.Fatal("unknown operation replayed", ok, e)
	}
	sub.Enabled = false
	sub.Input = json.RawMessage(`{"enabled":false}`)
	if _, e = s.SetNoticePolicy(ctx, sub, "r_unsubscribe", 1); e != nil {
		t.Fatal(e)
	}
	got, receipt, e := s.CurrentNoticePolicy(ctx, "scope", "m")
	if e != nil || got.Enabled || receipt.Revision != 2 {
		t.Fatal(got, receipt, e)
	}
}
func TestUnifiedConcurrentNoticeClaimHasOneWriter(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	p := StartPush{Scope: "scope", Manager: "m", EventID: "event", Payload: json.RawMessage(`{}`), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if e := s.PrepareStartPush(ctx, p); e != nil {
		t.Fatal(e)
	}
	ch := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func() { ok, _ := s.ClaimStartPush(ctx, p); ch <- ok }()
	}
	count := 0
	for i := 0; i < 20; i++ {
		if <-ch {
			count++
		}
	}
	if count != 1 {
		t.Fatal("not one original writer", count)
	}
}
func TestUnifiedConcurrentAdmissionAndResetCannotBothCommit(t *testing.T) {
	for i := 0; i < 20; i++ {
		s, _ := Open("")
		raw := json.RawMessage(`{}`)
		v, e := s.SaveContextReset(context.Background(), "p", "h", ContextReset{IntentID: "r_reset_original", SessionID: "s", ArchiveHash: workHash(raw)}, raw)
		if e != nil {
			t.Fatal(e)
		}
		gate := make(chan struct{})
		ch := make(chan bool, 2)
		go func() {
			<-gate
			_, e := s.ReserveContextReset(context.Background(), "p", v.IntentID, v.Revision)
			ch <- e == nil
		}()
		go func() {
			<-gate
			_, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: "new", Payload: []byte(`{}`)})
			ch <- e == nil
		}()
		close(gate)
		a, b := <-ch, <-ch
		if a && b {
			t.Fatal("reset and input committed concurrently")
		}
		s.Close()
	}
}

func TestUnifiedDurableStartJoinBeyondWindowsAndSubscriberLimit(t *testing.T) {
	s, e := Open("")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	// Rotation must reach every original policy, including positions >500.
	for i := 0; i < 601; i++ {
		if e = s.SubscribeStart(ctx, NoticeSubscriber{Scope: fmt.Sprintf("phone-%04d", i), Manager: "manager", Input: json.RawMessage(`{"enabled":true}`), Enabled: true}); e != nil {
			t.Fatal(e)
		}
	}
	seen := map[string]bool{}
	for tick := 0; tick < 32; tick++ {
		subs, e := s.StartJoinSubscribers(ctx, 20)
		if e != nil {
			t.Fatal(e)
		}
		for _, sub := range subs {
			seen[sub.Scope] = true
		}
	}
	if len(seen) != 601 {
		t.Fatal("subscriber silently truncated", len(seen))
	}
	sub := NoticeSubscriber{Scope: "phone-0000", Manager: "manager", Input: json.RawMessage(`{"enabled":true}`), Enabled: true}
	for i := 0; i < 105; i++ {
		p := liveStart(t, s, fmt.Sprintf("r_join_%03d", i))
		p.ObservedAt = time.Now().Add(-time.Hour).UnixMilli()
		if _, e = s.RecordNativeStart(p); e != nil {
			t.Fatal(e)
		}
	}
	count := 0
	for batch := 0; batch < 2; batch++ {
		facts, e := s.StartJoinFacts(ctx, sub, 100)
		if e != nil {
			t.Fatal(e)
		}
		for _, p := range facts {
			if e = s.CommitStartJoin(ctx, sub, p, "expired"); e != nil {
				t.Fatal(e)
			}
			count++
		}
	}
	if count != 105 {
		t.Fatal("native start silently truncated", count)
	}
	facts, e := s.StartJoinFacts(ctx, sub, 100)
	if e != nil || len(facts) != 0 {
		t.Fatal("durable sequence did not settle", len(facts), e)
	}
	var outcomes int
	if e = s.db.QueryRow(`SELECT COUNT(*) FROM jarvis_start_push WHERE scope=? AND manager=? AND state='expired'`, sub.Scope, sub.Manager).Scan(&outcomes); e != nil || outcomes != 105 {
		t.Fatal(outcomes, e)
	}
	// Exact JSON linkage does not stop at page 17 / item 510.
	for i := 0; i < 511; i++ {
		m := WorkMutation{IntentID: fmt.Sprintf("r_link_original_%04d", i), WorkItem: WorkItem{ID: fmt.Sprintf("item-%04d", i), Kind: "execution", Title: "original", RecordingState: "recorded"}}
		if i == 510 {
			m.Links = []WorkLink{{Kind: "intake", SessionID: "original-target", RequestID: "original-request"}}
		}
		if _, e = s.MutateWork(ctx, sub.Scope, sub.Manager, m); e != nil {
			t.Fatal(e)
		}
	}
	linked, e := s.HasWorkExecutionLink(ctx, sub.Scope, sub.Manager, "original-target", "original-request")
	if e != nil || !linked {
		t.Fatal("late original link lost", linked, e)
	}
}

func TestUnifiedWorkTitleUTF16RejectedBeforeClaim(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for i, title := range []string{strings.Repeat("😀", 101), strings.Repeat("中", 181)} {
		in := WorkMutation{IntentID: fmt.Sprintf("r_oversize_title_%d", i), WorkItem: WorkItem{ID: "title", Kind: "discussion_note", Title: title, RecordingState: "recorded"}}
		if _, e = s.MutateWork(context.Background(), "scope", "manager", in); e == nil {
			t.Fatal("oversize accepted")
		}
		if _, e = s.WorkOperation(context.Background(), "scope", "manager", in.IntentID); e == nil {
			t.Fatal("invalid title intent claimed")
		}
	}
	in := WorkMutation{IntentID: "r_valid_title_boundary", WorkItem: WorkItem{ID: "boundary", Kind: "discussion_note", Title: strings.Repeat("😀", 100), RecordingState: "recorded"}}
	if _, e = s.MutateWork(context.Background(), "scope", "manager", in); e != nil {
		t.Fatal(e)
	}
	in.IntentID = "r_title_marker_rejected"
	in.ID = "marker"
	in.TitleTruncated = true
	in.TitleHash = strings.Repeat("a", 64)
	if _, e = s.MutateWork(context.Background(), "scope", "manager", in); e == nil {
		t.Fatal("presentation accepted as raw metadata")
	}
}
