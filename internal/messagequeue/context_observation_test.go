package messagequeue

import (
	"everything-go/internal/protocol"
	"testing"
)

func TestContextObservationDurableAliasesReplayAndScope(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	scope := ContextScope{"authority", "session", "codex", "root", 0}
	zero, max := 0, 1000
	usage := &protocol.ContextUsageObservation{UsedTokens: &zero, MaxTokens: &max}
	record := func(id string, state string, aliases []string, at int64) {
		t.Helper()
		if err := s.RecordContextObservation(scope, id, nil, state, aliases, at); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.RecordContextObservation(scope, "usage-0", usage, "", nil, 100); err != nil {
		t.Fatal(err)
	}
	record("item-1", "observed", []string{"manual-op"}, 101)
	record("manual-op", "unknown", nil, 102)
	r, _, _ := s.ContextObservation(scope)
	if r.Completed != 0 {
		t.Fatal("pending/unknown counted")
	}
	record("manual-op", "completed", nil, 103)
	record("item-1", "completed", nil, 104)
	record("manual-op", "completed", nil, 105)
	r, _, _ = s.ContextObservation(scope)
	if r.Completed != 1 || r.InvalidatedAt != 103 || r.AsOf != 103 {
		t.Fatalf("double count/replay invalidation: %+v", r)
	}
	if err = s.RecordContextObservation(scope, "usage-0", usage, "", nil, 106); err != nil {
		t.Fatal(err)
	}
	r, _, _ = s.ContextObservation(scope)
	if r.UsageAt != 100 || r.InvalidatedAt != 103 {
		t.Fatal("replayed usage refreshed health")
	}
	if err = s.RecordContextObservation(scope, "usage-after", usage, "", nil, 107); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record("item-1", "completed", nil, 200)
	record("auto-item-2", "completed", nil, 201)
	r, _, _ = s.ContextObservation(scope)
	if r.Completed != 2 || r.TrackedSince != 100 || r.InvalidatedAt != 201 {
		t.Fatalf("restart: %+v", r)
	}
	for _, foreign := range []ContextScope{{"other", "session", "codex", "root", 0}, {"authority", "fork", "codex", "root", 0}, {"authority", "session", "codex", "child", 0}, {"authority", "session", "claude", "root", 0}} {
		_, found, e := s.ContextObservation(foreign)
		if e != nil || found {
			t.Fatal("foreign scope leaked")
		}
	}
	record("failed-item", "failed", nil, 202)
	if e := s.RecordContextObservation(scope, "failed-item", nil, "completed", nil, 203); e == nil {
		t.Fatal("failed proof promoted")
	}
	r, _, _ = s.ContextObservation(scope)
	if r.Completed != 2 {
		t.Fatal("failure counted")
	}
}
func TestContextObservationAtomicConflictAndSourceTime(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c := ContextScope{"a", "s", "codex", "t", 0}
	for _, id := range []string{"one", "two"} {
		if e = s.RecordContextObservation(c, id, nil, "completed", nil, 100); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.RecordContextObservation(c, "one", nil, "completed", []string{"two"}, 200); e == nil {
		t.Fatal("ambiguous canonical merge allowed")
	}
	nativeTime := int64(1)
	used := 30
	max := 100
	if e = s.RecordContextObservation(c, "usage-native", &protocol.ContextUsageObservation{UsedTokens: &used, MaxTokens: &max, ReportedAtMS: &nativeTime}, "", nil, 300); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.ContextObservation(c)
	if e != nil || r.Completed != 2 || r.UsageAt != 1 || r.AsOf != 300 || r.InvalidatedAt != 0 {
		t.Fatalf("native report time/atomicity: %+v %v", r, e)
	}
}

func TestContextDelayedUsageCannotClearConfirmedCompactionInvalidation(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	scope := ContextScope{"authority", "s", "codex", "root", 1}
	used, max := 10, 100
	// A newly observed usage record can have a provider timestamp preceding
	// the confirmed compaction even when it arrives later at this server.
	if e = s.RecordContextObservation(scope, "compact", nil, "completed", nil, 200); e != nil {
		t.Fatal(e)
	}
	before := int64(190)
	if e = s.RecordContextObservation(scope, "delayed", &protocol.ContextUsageObservation{UsedTokens: &used, MaxTokens: &max, ReportedAtMS: &before}, "", nil, 210); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.ContextObservation(scope)
	if e != nil || r.InvalidatedAt != 200 {
		t.Fatalf("old native usage cleared invalidation: %+v %v", r, e)
	}
}

func TestContextProviderTimeAfterCompactionClearsFenceDespiteDelayedPoll(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	scope := ContextScope{"authority", "s", "codex", "root", 1}
	used, max := 10, 100
	// Both observations arrive at 1000+, but their native source times are
	// 100 then 101. Server receipt time is not the native ordering frontier.
	if e = s.RecordContextObservation(scope, "compact", nil, "completed", nil, 1000, 100); e != nil {
		t.Fatal(e)
	}
	after := int64(101)
	if e = s.RecordContextObservation(scope, "after", &protocol.ContextUsageObservation{UsedTokens: &used, MaxTokens: &max, ReportedAtMS: &after}, "", nil, 1001); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.ContextObservation(scope)
	if e != nil || r.InvalidatedAt != 0 || r.UsageAt != 101 {
		t.Fatalf("new native report did not clear fence: %+v %v", r, e)
	}
}
