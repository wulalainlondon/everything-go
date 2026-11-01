package agentactivity

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

type publicFixtureRPC struct {
	*nativeTestRPC
	page                          string
	requestedChild, requestedTurn string
	onItem                        func()
}

func TestNativePublicReferenceRetainsOriginalAuthorityAndProof(t *testing.T) {
	var refs []NativePublicReturn
	for _, authority := range []string{"authority-a", "authority-b"} {
		scope := nativeTestScope
		scope.Authority = authority
		s := NewNativeSource(scope)
		if e := s.Observe(scope, nativeEvent("creation", "started")); e != nil {
			t.Fatal(e)
		}
		r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}
		r.scope = scope
		ref, e := s.PublicReturn(context.Background(), r, "child-thread", "real-native-turn")
		if e != nil || ref.Scope != scope || ref.SourceRevision != 1 {
			t.Fatal("original proof missing", e)
		}
		refs = append(refs, ref)
	}
	if reflect.DeepEqual(refs[0], refs[1]) {
		t.Fatal("same native IDs erased original authority proof")
	}
}

type retireBeforeItemRPC struct {
	*publicFixtureRPC
	scopeChecks int
}

func (r *retireBeforeItemRPC) Scope() NativeScope {
	r.scopeChecks++
	out := r.scope
	if r.scopeChecks == 3 {
		r.scope.Epoch = "other-valid-epoch"
	}
	return out
}
func TestNativePublicReturnRetiresBeforeItemSend(t *testing.T) {
	r := &retireBeforeItemRPC{publicFixtureRPC: &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}}
	ref, e := nativeSource(t).PublicReturn(context.Background(), r, "child-thread", "real-native-turn")
	if e == nil || ref.ItemID != "" || r.requestedChild != "" {
		t.Fatal("item read escaped retired membership proof")
	}
}

func (r *publicFixtureRPC) LatestTurnItem(_ context.Context, proof NativeReadProof, child, turn string) (json.RawMessage, error) {
	if r.scope != proof.Scope {
		return nil, ErrNativeChanged
	}
	release, e := proof.AcquireFor(child, turn)
	if e != nil {
		return nil, e
	}
	r.requestedChild, r.requestedTurn = child, turn
	release()
	if r.onItem != nil {
		r.onItem()
	}
	return []byte(r.page), nil
}

const publicPage = `{"data":[{"turnId":"real-native-turn","item":{"type":"agentMessage","id":"native-public-final","phase":"final_answer","text":"UNTRUSTED_PUBLIC_BODY"}}],"nextCursor":null}`

func TestNativePublicReturnRequiresOriginalCompletedTurnAndExactItem(t *testing.T) {
	s := nativeSource(t)
	r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}
	p, e := s.PublicReturn(context.Background(), r, "child-thread", "real-native-turn")
	if e != nil || p.ItemID != "native-public-final" || p.NativeTurn != "real-native-turn" || r.requestedChild != "child-thread" {
		t.Fatal("original public reference unavailable", e)
	}
	b, _ := json.Marshal(p)
	if strings.Contains(string(b), "UNTRUSTED_PUBLIC_BODY") {
		t.Fatal("body retained")
	}
	for _, identity := range [][2]string{{"foreign-child", "real-native-turn"}, {"child-thread", "invented-round"}} {
		r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}
		if p, e = s.PublicReturn(context.Background(), r, identity[0], identity[1]); e == nil || p.ItemID != "" || r.requestedChild != "" {
			t.Fatal("unproven turn queried")
		}
	}
	r = &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}
	r.turns = strings.Replace(r.turns, `"status":"completed"`, `"status":"inProgress"`, 1)
	if _, e = s.PublicReturn(context.Background(), r, "child-thread", "real-native-turn"); e == nil || r.requestedChild != "" {
		t.Fatal("active turn became final")
	}
}
func TestNativePublicReturnDoesNotSearchOrPromoteOtherOutput(t *testing.T) {
	for _, page := range []string{strings.Replace(publicPage, "final_answer", "commentary", 1), strings.Replace(publicPage, `"phase":"final_answer",`, "", 1), strings.Replace(publicPage, "agentMessage", "reasoning", 1), strings.Replace(publicPage, "real-native-turn", "other-round", 1), strings.Replace(publicPage, `"id":"native-public-final"`, `"id":"native-public-final","ID":"forged"`, 1), `{"data":[],"nextCursor":"more"}`} {
		r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: page}
		p, e := nativeSource(t).PublicReturn(context.Background(), r, "child-thread", "real-native-turn")
		if e == nil || p.ItemID != "" {
			t.Fatal("non-final/latest item promoted")
		}
	}
}
func TestNativePublicReturnInvalidatesAfterItemRead(t *testing.T) {
	for _, kind := range []string{"scope", "registry", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			s := nativeSource(t)
			r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: publicPage}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r.onItem = func() {
				switch kind {
				case "scope":
					r.scope.Epoch = "new"
				case "registry":
					_ = s.Observe(nativeTestScope, nativeEvent("late", "completed"))
				case "cancel":
					cancel()
				}
			}
			p, e := s.PublicReturn(ctx, r, "child-thread", "real-native-turn")
			if e == nil || p.ItemID != "" {
				t.Fatal("stale public reference escaped")
			}
		})
	}
}
func TestNativeMailboxWaitIsOnlyTheOriginalBlockingCall(t *testing.T) {
	start := `{"threadId":"root-thread","turnId":"parent-turn","item":{"type":"collabAgentToolCall","id":"wait-call","tool":"wait","status":"inProgress","senderThreadId":"root-thread","receiverThreadIds":["qa"],"agentsStates":{"qa":{"status":"completed","message":"not canonical review"}}}}`
	w, e := DecodeNativeMailboxWait("item/started", []byte(start), "root-thread")
	if e != nil || w.Phase != "blocking" || w.CallID != "wait-call" {
		t.Fatal(e)
	}
	exit := strings.Replace(start, `"status":"inProgress"`, `"status":"completed"`, 1)
	w, e = DecodeNativeMailboxWait("item/completed", []byte(exit), "root-thread")
	if e != nil || w.Phase != "exited" {
		t.Fatal("wait did not exit")
	}
	for _, b := range []string{strings.Replace(start, "root-thread", "foreign-root", 1), strings.Replace(start, `"tool":"wait"`, `"tool":"followupTask"`, 1), strings.Replace(start, `"id":"wait-call"`, `"id":"wait-call","ID":"forged"`, 1), exit} {
		if _, e = DecodeNativeMailboxWait("item/started", []byte(b), "root-thread"); e == nil {
			t.Fatal("foreign/nonblocking wait accepted")
		}
	}
}
