package history

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMediaLoadingWitnessSeesAppendWithoutWindowScan(t *testing.T) {
	t.Setenv("EVERYTHING_GO_HISTORY_GENERATION_DIR", t.TempDir())
	path := filepath.Join(t.TempDir(), "fixture.jsonl")
	if e := os.WriteFile(path, []byte("{\"type\":\"fixture\"}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	generation, revision, e := FileWindowWitness(path)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteString("{\"type\":\"append\"}\n")
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	nextGeneration, nextRevision, e := FileWindowWitness(path)
	if e != nil {
		t.Fatal(e)
	}
	if nextGeneration != generation || nextRevision == revision {
		t.Fatal("append did not preservegeneration/change revision")
	}
	windowFiles.Lock()
	_, scanned := windowFiles.values[path]
	windowFiles.Unlock()
	if scanned {
		t.Fatal("metadata witness scanned history offsets")
	}
}
