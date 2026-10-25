package workspace

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObservedStructuredResearchURLsDecodeAndExcludeThumbnails(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"query": "original", "results": []map[string]string{{"url": "https://example.org/?a=1&b=2", "thumbnail_url": "https://unrelated.example/image", "snippet": "untrusted data"}, {"url": "https://user:password@example.org/private"}, {"url": "javascript:alert(1)"}}})
	urls := observationURLs(string(raw))
	if len(urls) != 1 || urls[0] != "https://example.org/?a=1&b=2" {
		t.Fatalf("source URLs lost or unsafe: %v", urls)
	}
}
func TestResearchObservationBoundariesAndLateStartPreserveResult(t *testing.T) {
	f := setup(t)
	agent := f.agent("Research", "native-observed", true)
	entity := f.task(agent, nil)
	task, _ := bodyOf[Task](entity)
	var err error
	if err = f.s.RecordResearchTool(f.ctx, task.SessionID, task.RequestID, "web-observed", "webSearch", "", "", false); err != nil {
		t.Fatal(err)
	}
	out := `{"query":"query","results":[{"url":"https://example.org/","snippet":"` + strings.Repeat("界", 6000) + `"}]}`
	if err = f.s.RecordResearchTool(f.ctx, task.SessionID, task.RequestID, "web-observed", "", "", out, true); err != nil {
		t.Fatal(err)
	}
	// Existing helper fixture APIs differ across suites; use task source reader below.
	refs, err := researchReferences(f.ctx, f.s.db, f.room.ID, entity.ID)
	if err != nil || len(refs) != 1 {
		t.Fatalf("refs %v %v", refs, err)
	}
	e, err := getEntity(f.ctx, f.s.db, f.room.ID, refs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var o ResearchObservation
	json.Unmarshal(e.Body, &o)
	if o.Query != "query" || !o.Truncated || len(o.Excerpt) > 16000 || o.OutputHash != digest([]byte(out)) || len(o.URLs) != 1 {
		t.Fatalf("incomplete observation %+v", o)
	}
	if err = f.s.RecordResearchTool(f.ctx, task.SessionID, task.RequestID, "web-observed", "webSearch", "query", "", false); err != nil {
		t.Fatal(err)
	}
	after, _ := getEntity(f.ctx, f.s.db, f.room.ID, e.ID)
	if after.Hash != e.Hash {
		t.Fatal("late start downgraded final observation")
	}
}
