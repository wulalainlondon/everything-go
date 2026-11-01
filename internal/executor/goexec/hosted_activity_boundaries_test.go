package goexec

import (
	"encoding/json"
	"everything-go/internal/agentactivity"
	"strings"
	"testing"
)

func TestHostedNativeParentBoundaryExcludesLateChildEvents(t *testing.T) {
	raw, e := agentactivity.DecodeRolloutActivity([]byte(recoveryEvent()), exactFixtureThread)
	if e != nil {
		t.Fatal(e)
	}
	var b hostedBoundaries
	b.activity(raw, exactFixtureThread)
	b.terminal(exactFixtureThread, "turn")
	boundary := b.views["turn"]
	late := strings.Replace(string(raw), `"id":"creation"`, `"id":"late"`, 1)
	b.activity([]byte(late), exactFixtureThread)
	b.terminal(exactFixtureThread, "turn")
	if len(b.views["turn"].ActivityIDs) != 1 || b.views["turn"].ActivityIDs[0] != "creation" || b.views["turn"].BoundaryID != boundary.BoundaryID {
		t.Fatal("late event rewrote parent as-of")
	}
	var replay hostedBoundaries
	replay.activity(raw, exactFixtureThread)
	replay.terminal(exactFixtureThread, "turn")
	replay.activity([]byte(late), exactFixtureThread)
	a, _ := json.Marshal(b.views["turn"])
	r, _ := json.Marshal(replay.views["turn"])
	if string(a) != string(r) {
		t.Fatal("restart duplicated parent boundary")
	}
}
