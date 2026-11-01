package agentactivity

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

var nativeTestScope = NativeScope{Authority: "authority", Session: "session", RootThread: "root-thread", Epoch: "boot:1:0"}

func nativeEvent(id, kind string) []byte {
	b, _ := json.Marshal(map[string]any{"threadId": "root-thread", "turnId": "parent-turn", "completedAtMs": 1000, "item": map[string]any{"type": "subAgentActivity", "id": id, "kind": kind, "agentThreadId": "child-thread", "agentPath": "/root/worker"}})
	return b
}

type nativeTestRPC struct {
	scope              NativeScope
	root, child, turns string
	reads              []string
	after              func(string)
	metadata           map[string]string
	failAt             string
}

func nativeFixtureRPC() *nativeTestRPC {
	return &nativeTestRPC{scope: nativeTestScope, root: `{"thread":{"id":"root-thread","sessionId":"native-tree","turns":[]}}`, child: `{"thread":{"id":"child-thread","parentThreadId":"root-thread","sessionId":"native-tree","turns":[]}}`, turns: `{"data":[{"id":"real-native-turn","rootTurnId":"parent-turn","status":"completed","items":[],"itemsView":"notLoaded","startedAt":1,"completedAt":2}],"nextCursor":null}`}
}
func (r *nativeTestRPC) Scope() NativeScope { return r.scope }
func (r *nativeTestRPC) ReadThread(_ context.Context, id string) (json.RawMessage, error) {
	r.reads = append(r.reads, id)
	if r.after != nil {
		r.after(id)
	}
	if r.failAt == id {
		return nil, ErrNativeSource
	}
	if v, ok := r.metadata[id]; ok {
		return []byte(v), nil
	}
	if id == nativeTestScope.RootThread {
		return []byte(r.root), nil
	}
	return []byte(r.child), nil
}
func (r *nativeTestRPC) ListTurns(_ context.Context, id string) (json.RawMessage, error) {
	r.reads = append(r.reads, id+":turns")
	if r.after != nil {
		r.after(id + ":turns")
	}
	if r.failAt == id+":turns" {
		return nil, ErrNativeSource
	}
	return []byte(r.turns), nil
}

func TestNativeMultiActorAllFailureExitsDiscardEverything(t *testing.T) {
	for _, mode := range []string{"rpc_epoch", "cancel", "second_read_error", "second_decode_error", "second_parent_error", "second_turn_read_error"} {
		t.Run(mode, func(t *testing.T) {
			s := nativeSource(t)
			b := strings.ReplaceAll(string(nativeEvent("activity2", "started")), "child-thread", "later-child")
			b = strings.ReplaceAll(b, "/root/worker", "/root/later")
			if e := s.Observe(nativeTestScope, []byte(b)); e != nil {
				t.Fatal(e)
			}
			r := nativeFixtureRPC()
			r.metadata = map[string]string{"later-child": strings.ReplaceAll(r.child, "child-thread", "later-child")}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r.after = func(id string) {
				if id != "child-thread:turns" {
					return
				}
				switch mode {
				case "rpc_epoch":
					r.scope.Epoch = "new"
				case "cancel":
					cancel()
				case "second_read_error":
					r.failAt = "later-child"
				case "second_decode_error":
					r.metadata["later-child"] = "{}"
				case "second_parent_error":
					r.metadata["later-child"] = strings.Replace(r.metadata["later-child"], `"parentThreadId":"root-thread"`, `"parentThreadId":"foreign-root"`, 1)
				case "second_turn_read_error":
					r.failAt = "later-child:turns"
				}
			}
			p, e := s.Probe(ctx, r)
			if e == nil || len(p.Actors) != 0 || p.SourceRevision != 0 {
				t.Fatalf("invalid collection escaped: %+v %v", p, e)
			}
		})
	}
}
func nativeSource(t *testing.T) *NativeSource {
	t.Helper()
	s := NewNativeSource(nativeTestScope)
	if e := s.Observe(nativeTestScope, nativeEvent("activity1", "started")); e != nil {
		t.Fatal(e)
	}
	return s
}

func TestNativeSourceUsesActualTurnsNotActivityOrDirectiveCounts(t *testing.T) {
	s := nativeSource(t)
	rpc := nativeFixtureRPC()
	for _, id := range []string{"followup1", "followup2", "followup3"} {
		if e := s.Observe(nativeTestScope, nativeEvent(id, "interacted")); e != nil {
			t.Fatal(e)
		}
	}
	p, e := s.Probe(context.Background(), rpc)
	if e != nil {
		t.Fatal(e)
	}
	a := p.Actors[0]
	if len(a.Activities) != 4 || len(a.Rounds) != 1 || a.Rounds[0].ID != "real-native-turn" || a.Rounds[0].ParentAssociation != "native_root_chain" || p.InventoryComplete || a.RoundsTruncated {
		t.Fatalf("invalid original native projection: %+v", p)
	}
	if strings.Join(rpc.reads, ",") != "root-thread,child-thread,child-thread:turns" {
		t.Fatal("read escaped proven source membership")
	}
}
func TestNativeCreationMetadataHasNoPerRoundEffectivePromotion(t *testing.T) {
	s := NewNativeSource(nativeTestScope)
	b := strings.Replace(string(nativeEvent("creation", "started")), `"kind":"started"`, `"kind":"started","model":"gpt-request-independent","reasoningEffort":"high"`, 1)
	if e := s.Observe(nativeTestScope, []byte(b)); e != nil {
		t.Fatal(e)
	}
	p, e := s.Probe(context.Background(), nativeFixtureRPC())
	if e != nil {
		t.Fatal(e)
	}
	if *p.Actors[0].Activities[0].ResolvedCreationModel != "gpt-request-independent" || *p.Actors[0].Activities[0].ResolvedCreationEffort != "high" {
		t.Fatal("lost separately resolved creation fields")
	}
	*p.Actors[0].Activities[0].ResolvedCreationModel = "mutated-output"
	p, e = s.Probe(context.Background(), nativeFixtureRPC())
	if e != nil || *p.Actors[0].Activities[0].ResolvedCreationModel != "gpt-request-independent" {
		t.Fatal("output mutated source")
	}
	if _, e = DecodeNativeActivity([]byte(strings.Replace(b, `"kind":"started"`, `"kind":"completed"`, 1)), nativeTestScope.RootThread); e == nil {
		t.Fatal("creation values accepted for another activity")
	}
}
func TestNativeRootChainIsNotUniqueImmediateParentOwnership(t *testing.T) {
	s := nativeSource(t)
	r := nativeFixtureRPC()
	r.turns = `{"data":[{"id":"turn1","rootTurnId":"parent-turn","status":"completed","items":[],"itemsView":"notLoaded"},{"id":"turn2","rootTurnId":"parent-turn","status":"inProgress","items":[],"itemsView":"notLoaded"},{"id":"turn3","rootTurnId":"older-chain","status":"failed","items":[],"itemsView":"notLoaded"}],"nextCursor":"opaque-next"}`
	p, e := s.Probe(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	a := p.Actors[0]
	if len(a.Rounds) != 3 || !a.RoundsTruncated || a.Rounds[0].ParentAssociation != "native_root_chain" || a.Rounds[1].ParentAssociation != "native_root_chain" || a.Rounds[2].ParentAssociation != "unresolved" {
		t.Fatal("root chain promoted or unmapped round reassigned")
	}
}
func TestNativeScopeAndSourceChangesDiscardProbe(t *testing.T) {
	for _, mode := range []string{"rpc_epoch", "new_activity", "failed_source", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s := nativeSource(t)
			r := nativeFixtureRPC()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r.after = func(id string) {
				if id != "child-thread:turns" {
					return
				}
				switch mode {
				case "rpc_epoch":
					r.scope.Epoch = "new"
				case "new_activity":
					_ = s.Observe(nativeTestScope, nativeEvent("later", "completed"))
				case "failed_source":
					_ = s.Observe(nativeTestScope, nativeEvent("activity1", "completed"))
				case "canceled":
					cancel()
				}
			}
			p, e := s.Probe(ctx, r)
			if e == nil || len(p.Actors) != 0 {
				t.Fatal("mixed/revoked source escaped")
			}
		})
	}
}
func TestNativeMetadataAndRoundBoundariesFailClosed(t *testing.T) {
	cases := map[string]func(*nativeTestRPC){
		"foreign_tree": func(r *nativeTestRPC) { r.child = strings.Replace(r.child, "native-tree", "other-tree", 1) },
		"foreign_parent": func(r *nativeTestRPC) {
			r.child = strings.Replace(r.child, `"parentThreadId":"root-thread"`, `"parentThreadId":"other-root"`, 1)
		},
		"history_returned":       func(r *nativeTestRPC) { r.child = strings.Replace(r.child, `"turns":[]`, `"turns":[{}]`, 1) },
		"missing_metadata_turns": func(r *nativeTestRPC) { r.child = strings.Replace(r.child, `,"turns":[]`, "", 1) },
		"missing_items":          func(r *nativeTestRPC) { r.turns = strings.Replace(r.turns, `"items":[],`, "", 1) },
		"wrong_items_view":       func(r *nativeTestRPC) { r.turns = strings.Replace(r.turns, "notLoaded", "full", 1) },
		"missing_data":           func(r *nativeTestRPC) { r.turns = `{"nextCursor":null}` },
		"missing_cursor":         func(r *nativeTestRPC) { r.turns = `{"data":[]}` },
		"ambiguous_key": func(r *nativeTestRPC) {
			r.turns = strings.Replace(r.turns, `"status":"completed"`, `"status":"completed","STATUS":"failed"`, 1)
		},
		"unknown_lifecycle": func(r *nativeTestRPC) { r.turns = strings.Replace(r.turns, "completed", "closed", 1) },
		"negative_time":     func(r *nativeTestRPC) { r.turns = strings.Replace(r.turns, `"startedAt":1`, `"startedAt":-1`, 1) },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			r := nativeFixtureRPC()
			edit(r)
			if _, e := nativeSource(t).Probe(context.Background(), r); e == nil {
				t.Fatal("malformed native source accepted")
			}
		})
	}
}
func TestNativeActivityConflictBoundsAndIdentity(t *testing.T) {
	for _, replace := range [][2]string{{"root-thread", "foreign-root"}, {`"agentPath":"/root/worker"`, `"agentPath":"/root/../worker"`}, {`"completedAtMs":1000`, `"completedAtMs":0`}, {`"kind":"started"`, `"kind":"invented"`}, {`"id":"activity1"`, `"id":"activity1","ID":"other"`}} {
		if _, e := DecodeNativeActivity([]byte(strings.Replace(string(nativeEvent("activity1", "started")), replace[0], replace[1], 1)), nativeTestScope.RootThread); e == nil {
			t.Fatal("unsafe activity admitted")
		}
	}
	s := nativeSource(t)
	if e := s.Observe(nativeTestScope, nativeEvent("activity1", "started")); e != nil {
		t.Fatal("exact replay should be idempotent")
	}
	if e := s.Observe(nativeTestScope, nativeEvent("activity1", "completed")); e == nil {
		t.Fatal("conflict should quarantine")
	}
	if _, e := s.Probe(context.Background(), nativeFixtureRPC()); e == nil {
		t.Fatal("conflicted source should not read")
	}
	s = NewNativeSource(nativeTestScope)
	for i := 0; i < 1024; i++ {
		b := nativeEvent(strings.Repeat("x", i/100+1)+string(rune('a'+i%100)), "started")
		if e := s.Observe(nativeTestScope, b); e != nil {
			t.Fatal(i, e)
		}
	}
	if e := s.Observe(nativeTestScope, nativeEvent("overflow", "started")); e == nil {
		t.Fatal("unbounded native activity retained")
	}
}
func TestNativeRolloutConversionRequiresExactPublicNativeItem(t *testing.T) {
	raw := `{"type":"event_msg","payload":{"type":"item_completed","thread_id":"root-thread","turn_id":"parent-turn","completed_at_ms":1000,"item":{"type":"SubAgentActivity","id":"event","kind":"started","agent_thread_id":"child-thread","agent_path":"/root/worker","model":"resolved-creation","reasoning_effort":"high"}}}`
	b, e := DecodeRolloutActivity([]byte(raw), nativeTestScope.RootThread)
	if e != nil {
		t.Fatal(e)
	}
	a, e := DecodeNativeActivity(b, nativeTestScope.RootThread)
	if e != nil || *a.ResolvedCreationModel != "resolved-creation" {
		t.Fatal("native persisted projection failed")
	}
	for _, r := range []string{strings.Replace(raw, "event_msg", "response_item", 1), strings.Replace(raw, "SubAgentActivity", "agent_message", 1), strings.Replace(raw, `"kind":"started"`, `"kind":"started","KIND":"completed"`, 1)} {
		if _, e := DecodeRolloutActivity([]byte(r), nativeTestScope.RootThread); e == nil {
			t.Fatal("foreign ambiguous persisted source accepted")
		}
	}
}
