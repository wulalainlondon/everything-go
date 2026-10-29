package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSourceRevisionStoreRoundtripAndUnknownFields(t *testing.T) {
	for _, value := range []string{"missing", "0", "9"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			field := ""
			if value != "missing" {
				field = `,"source_revision":` + value
			}
			before := `{"s":{"name":"original","resume_id":"thread","claude_uuid":"thread","backend":"codex","config_revision":0,"custom":{"keep":true}` + field + `}}`
			if err := os.WriteFile(path, []byte(before), 0600); err != nil {
				t.Fatal(err)
			}
			reg := NewRegistry()
			store := NewStore(path)
			reg.AttachStore(store)
			s, ok := reg.Get("s")
			if !ok {
				t.Fatal("not restored")
			}
			expected := uint64(0)
			if value == "9" {
				expected = 9
			}
			if s.Snapshot().SourceRevision != expected {
				t.Fatal(s.Snapshot())
			}
			if err := store.Save(reg.List()); err != nil {
				t.Fatal(err)
			}
			loaded := NewStore(path).Load()
			if loaded["s"].SourceRevision != expected {
				t.Fatal(loaded)
			}
			var raw map[string]map[string]json.RawMessage
			b, _ := os.ReadFile(path)
			json.Unmarshal(b, &raw)
			var kept struct {
				Keep bool `json:"keep"`
			}
			json.Unmarshal(raw["s"]["custom"], &kept)
			if string(raw["s"]["source_revision"]) != map[bool]string{true: "9", false: "0"}[value == "9"] || !kept.Keep {
				t.Fatal(string(b))
			}
		})
	}
	reg := NewRegistry()
	s := reg.Create("fresh", "x", "/work", "codex", "", "", "old")
	s.SetResumeID("new")
	store := NewStore(filepath.Join(t.TempDir(), "sessions.json"))
	if err := store.Save(reg.List()); err != nil {
		t.Fatal(err)
	}
	if store.Load()[s.ID].SourceRevision != s.Snapshot().SourceRevision || s.Snapshot().SourceRevision == 0 {
		t.Fatal("live generation lost")
	}
}

func TestSourceRevisionInvalidNeverBecomesRestoredZeroOrDeleted(t *testing.T) {
	for _, value := range []string{"null", "-1", "1.5", `"2"`} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			b := []byte(`{"s":{"name":"bad","resume_id":"thread","backend":"codex","source_revision":` + value + `}}`)
			os.WriteFile(path, b, 0600)
			store := NewStore(path)
			reg := NewRegistry()
			reg.AttachStore(store)
			if _, ok := reg.Get("s"); ok {
				t.Fatal("invalid source restored")
			}
			if err := store.Save(reg.List()); err != nil {
				t.Fatal(err)
			}
			var before, after any
			json.Unmarshal(b, &before)
			v, _ := os.ReadFile(path)
			json.Unmarshal(v, &after)
			x, _ := json.Marshal(before)
			y, _ := json.Marshal(after)
			if string(x) != string(y) {
				t.Fatal("invalid original changed")
			}
		})
	}
}

func TestSourceRevisionBackfillOnlyExactMissingField(t *testing.T) {
	for _, mode := range []string{"valid", "stale", "present", "wrong-thread", "hidden", "zero"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			extra := ""
			if mode == "present" {
				extra = `,"source_revision":0`
			}
			if mode == "hidden" {
				extra = `,"hidden":true`
			}
			b := []byte(`{"s":{"name":"original","resume_id":"thread","claude_uuid":"thread","config_revision":0,"backend":"codex","custom":[1,2]` + extra + `},"other":{"custom":"untouched"}}`)
			os.WriteFile(path, b, 0600)
			sum := sha256.Sum256(b)
			digest := hex.EncodeToString(sum[:])
			if mode == "stale" {
				digest = string(make([]byte, 64))
			}
			thread := "thread"
			if mode == "wrong-thread" {
				thread = "other"
			}
			gen := uint64(2)
			if mode == "zero" {
				gen = 0
			}
			err := NewStore(path).BackfillMissingSourceRevision(digest, "s", thread, 0, gen)
			after, _ := os.ReadFile(path)
			if mode != "valid" {
				if err == nil || string(after) != string(b) {
					t.Fatal("non-missing/mismatched source changed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var old, now map[string]map[string]json.RawMessage
			json.Unmarshal(b, &old)
			json.Unmarshal(after, &now)
			if string(now["s"]["source_revision"]) != "2" {
				t.Fatal(string(after))
			}
			delete(now["s"], "source_revision")
			x, _ := json.Marshal(old)
			y, _ := json.Marshal(now)
			if string(x) != string(y) {
				t.Fatal("unrelated data changed")
			}
			if err = NewStore(path).BackfillMissingSourceRevision(digest, "s", thread, 0, gen); err == nil {
				t.Fatal("stale operation replayed")
			}
		})
	}
}

func TestSourceRevisionInvalidReloadDoesNotRetainDeletionOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	os.WriteFile(path, []byte(`{"s":{"resume_id":"thread","backend":"codex","source_revision":2}}`), 0600)
	store := NewStore(path)
	if len(store.Load()) != 1 {
		t.Fatal("missing valid source")
	}
	bad := []byte(`{"s":{"resume_id":"thread","backend":"codex","source_revision":null}}`)
	os.WriteFile(path, bad, 0600)
	if len(store.Load()) != 0 {
		t.Fatal("invalid became zero")
	}
	if err := store.Save(nil); err != nil {
		t.Fatal(err)
	}
	var now map[string]any
	raw, _ := os.ReadFile(path)
	json.Unmarshal(raw, &now)
	if _, ok := now["s"]; !ok {
		t.Fatal("original invalid source deleted")
	}
}

func TestSourceRevisionBackfillRejectsUnknownConfigurationAndVisibility(t *testing.T) {
	for _, field := range []string{`"config_revision":"unknown"`, `"config_revision":null`, `"config_revision":-1`, `"config_revision":1.5`, `"config_revision":0,"hidden":"unknown"`, `"config_revision":0,"hidden":null`} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			raw := []byte(`{"s":{"name":"original","resume_id":"thread","claude_uuid":"thread","backend":"codex",` + field + `}}`)
			os.WriteFile(path, raw, 0600)
			sum := sha256.Sum256(raw)
			if err := NewStore(path).BackfillMissingSourceRevision(hex.EncodeToString(sum[:]), "s", "thread", 0, 2); err == nil {
				t.Fatal("unknown became authorized default")
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(raw) {
				t.Fatal("negative write")
			}
		})
	}
}
