package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestContextEligibilityFenceRoundtripAndFutureDefault(t *testing.T) {
	r := NewRegistry()
	s := r.Create("s", "Root", "/work", "codex", "old", "", "")
	s.SetResumeID("root")
	if s.Snapshot().ContextNotBeforeMS != 0 {
		t.Fatal("initial binding invented a configuration change time")
	}
	s.SetModel("new")
	before := s.Snapshot()
	if before.ContextNotBeforeMS <= 0 {
		t.Fatal("model change lacks source-time fence")
	}
	path := filepath.Join(t.TempDir(), "saved.json")
	store := NewStore(path)
	if e := store.Save(r.List()); e != nil {
		t.Fatal(e)
	}
	rr := NewRegistry()
	rr.AttachStore(NewStore(path))
	restored, _ := rr.Get("s")
	if restored.Snapshot().ContextNotBeforeMS != before.ContextNotBeforeMS || restored.Snapshot().SourceRevision != before.SourceRevision {
		t.Fatal("configuration eligibility lost on restore")
	}
	cfg := ConfigurationFrom(restored.Snapshot())
	cfg.Model = "future"
	if _, e := restored.SetFutureConfiguration(cfg, nil); e != nil {
		t.Fatal(e)
	}
	if e := NewStore(path).Save(rr.List()); e != nil {
		t.Fatal(e)
	}
	loaded := NewStore(path).Load()["s"]
	if loaded.Model != "future" || loaded.ContextNotBeforeMS < before.ContextNotBeforeMS {
		t.Fatal("future default restored without report eligibility fence")
	}
}
func TestContextMalformedEligibilityTimePreservesRawEntry(t *testing.T) {
	for _, bad := range []string{"null", "-1", "1.5", `"123"`} {
		path := filepath.Join(t.TempDir(), "saved.json")
		raw := []byte(`{"s":{"resume_id":"root","backend":"codex","context_not_before_ms":` + bad + `,"custom":"keep"}}`)
		if e := os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
		st := NewStore(path)
		if len(st.Load()) != 0 {
			t.Fatal("corrupt eligibility restored as known zero")
		}
		if e := st.Save(nil); e != nil {
			t.Fatal(e)
		}
		out, _ := os.ReadFile(path)
		var v map[string]map[string]json.RawMessage
		json.Unmarshal(out, &v)
		if string(v["s"]["context_not_before_ms"]) != bad {
			t.Fatal("corrupt unowned entry overwritten")
		}
	}
}
