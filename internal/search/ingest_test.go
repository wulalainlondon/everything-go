package search

import (
	"os"
	"path/filepath"
	"testing"
)

type countingSource struct {
	path      string
	metaReads int
	include   bool
}

func (s *countingSource) name() string       { return "codex" }
func (s *countingSource) enabled() bool      { return true }
func (s *countingSource) discover() []string { return []string{s.path} }
func (s *countingSource) iterMessages(path string, _ int64) ([]searchableMessage, int64) {
	info, _ := os.Stat(path)
	return nil, info.Size()
}

func TestIngestMetricsDistinguishChangedAndIdlePasses(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-metrics.jsonl")
	contents := []byte("{}\n{}\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	idx.sources = []source{&countingSource{path: path, include: true}}

	changed := idx.ingestAllMetrics()
	if changed.FilesSeen != 1 || changed.FilesChanged != 1 || changed.FilesQueued != 1 || changed.BytesRead != int64(len(contents)) {
		t.Fatalf("changed metrics=%+v", changed)
	}
	idle := idx.ingestAllMetrics()
	if idle.FilesSeen != 1 || idle.FilesChanged != 0 || idle.FilesQueued != 0 || idle.BytesRead != 0 {
		t.Fatalf("idle metrics=%+v", idle)
	}
}
func (s *countingSource) headSignature(string) string { return "head" }
func (s *countingSource) sessionIDFor(string) string  { return "codex:test" }
func (s *countingSource) sessionMeta(string) sessionMeta {
	s.metaReads++
	return sessionMeta{Cwd: "/tmp", DisplayName: "test", FirstTS: "2026-01-01"}
}
func (s *countingSource) includeMeta(sessionMeta) bool { return s.include }

func TestDiscoverJobsSkipsMetadataForUnchangedFiles(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-test.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	if _, err := idx.db.Exec(
		`INSERT INTO ingest_state(source_path,file_size,last_mtime,last_offset,head_sha256,last_ingest_at,msg_extracted,errors)
		 VALUES(?,?,?,?,?,?,?,?)`, path, info.Size(), mtime, info.Size(), "head", mtime, 0, 0,
	); err != nil {
		t.Fatal(err)
	}
	src := &countingSource{path: path, include: true}
	idx.sources = []source{src}
	if jobs := idx.discoverJobs(); len(jobs) != 0 {
		t.Fatalf("unchanged file produced jobs: %+v", jobs)
	}
	if src.metaReads != 0 {
		t.Fatalf("unchanged file parsed metadata %d time(s)", src.metaReads)
	}

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString("{}\n")
	_ = f.Close()
	if jobs := idx.discoverJobs(); len(jobs) != 1 {
		t.Fatalf("changed file jobs=%d, want 1", len(jobs))
	}
	if src.metaReads != 1 {
		t.Fatalf("changed file metadata reads=%d, want 1", src.metaReads)
	}
}

func TestDiscoverJobsPersistsSkippedPolicyCursor(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "ignored.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := &countingSource{path: path, include: false}
	idx.sources = []source{src}
	if jobs := idx.discoverJobs(); len(jobs) != 0 || src.metaReads != 1 {
		t.Fatalf("first ignored discovery jobs=%d reads=%d", len(jobs), src.metaReads)
	}
	if jobs := idx.discoverJobs(); len(jobs) != 0 || src.metaReads != 1 {
		t.Fatalf("unchanged ignored file was reparsed jobs=%d reads=%d", len(jobs), src.metaReads)
	}
}

func TestLineReaderUntilStopsPhysicalRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "many.jsonl")
	if err := os.WriteFile(path, []byte("one\ntwo\nthree\nfour\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	offset := lineReaderUntil(path, 0, func(_ []byte, _ int, _ int64) bool {
		calls++
		return calls == 2
	})
	if calls != 2 || offset != int64(len("one\ntwo\n")) {
		t.Fatalf("early stop calls=%d offset=%d", calls, offset)
	}
}
