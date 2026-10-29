package messagequeue

import (
	"context"
	"strings"
	"testing"
)

func TestControllerResultAdmissionOriginalTransactionTerminalAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	p := []byte(`{"content":"frozen references only","configuration":{"revision":0},"message_purpose":"result_return"}`)
	admission := &ControllerResultAdmission{Batch: "batch", Scope: strings.Repeat("a", 64), Session: "parent", Request: "request"}
	first, inserted, e := s.Enqueue(Entry{SessionID: "parent", RequestID: "request", Payload: p, ControllerResult: admission})
	if e != nil || !inserted {
		t.Fatal(e)
	}
	if _, _, e = s.Transition("parent", "request", []State{Queued}, Completed, "", "", ""); e != nil {
		t.Fatal(e)
	}
	final, found, e := s.Get("parent", "request")
	if e != nil || !found || len(final.Payload) != 0 || final.PayloadHash != first.PayloadHash {
		t.Fatal(final, e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	proof, found, e := s.ControllerResultInput(context.Background(), "parent", "request")
	if e != nil || !found || string(proof.Payload) != string(p) || proof.IntentHash != final.PayloadHash {
		t.Fatal(proof, e)
	}
	// A colliding replay is not allowed to rewrite/backfill the admission proof.
	altered := *admission
	altered.Scope = strings.Repeat("b", 64)
	if _, created, e := s.Enqueue(Entry{SessionID: "parent", RequestID: "request", Payload: p, ControllerResult: &altered}); e != nil || created {
		t.Fatal(created, e)
	}
	after, _, e := s.ControllerResultInput(context.Background(), "parent", "request")
	if e != nil || after.Scope != proof.Scope {
		t.Fatal(after, e)
	}
	if _, _, e = s.Enqueue(Entry{SessionID: "parent", RequestID: "wrong", Payload: p, ControllerResult: admission}); e == nil {
		t.Fatal("wrong request admitted")
	}
	if _, found, e = s.Get("parent", "wrong"); e != nil || found {
		t.Fatal("partial admission", found, e)
	}
	if _, found, e = s.ControllerResultInput(context.Background(), "parent", "wrong"); e != nil || found {
		t.Fatal("partial proof", found, e)
	}
	if _, _, e = s.Enqueue(Entry{SessionID: "parent", RequestID: "old-ordinary", Payload: p}); e != nil {
		t.Fatal(e)
	}
	if _, found, e = s.ControllerResultInput(context.Background(), "parent", "old-ordinary"); e != nil || found {
		t.Fatal("unrelated row fabricated proof", found, e)
	}
}
