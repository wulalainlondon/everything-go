package session

import (
	"testing"
	"time"
)

func TestUnifiedReorderOriginalActorMailboxActualDequeue(t *testing.T) {
	s := newIdle("s")
	defer s.Close()
	release := s.HoldQueue()
	ran := make(chan string, 4)
	for _, value := range []string{"a", "manual", "b", "c"} {
		id := value
		if !s.SubmitNamed(id, func() { ran <- id; s.EndTurn() }) {
			t.Fatal("submit")
		}
	}
	expected := s.WaitingIDs()
	if e := s.ReorderWaiting(expected, []string{"a", "manual", "c", "b"}); e != nil {
		t.Fatal(e)
	}
	release()
	for _, want := range []string{"a", "manual", "c", "b"} {
		select {
		case got := <-ran:
			if got != want {
				t.Fatal(got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("actor did not dequeue")
		}
	}
	if e := s.ReorderWaiting(nil, nil); e == nil {
		t.Fatal("unfenced reorder")
	}
}
