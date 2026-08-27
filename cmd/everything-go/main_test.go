package main

import (
	"testing"
	"time"
)

func TestIndexBackoff(t *testing.T) {
	min := time.Minute
	max := 15 * time.Minute
	wants := []time.Duration{time.Minute, time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for idle, want := range wants {
		if got := indexBackoff(min, max, idle); got != want {
			t.Fatalf("idle=%d delay=%s want=%s", idle, got, want)
		}
	}
	if got := indexBackoff(min, max, -1); got != min {
		t.Fatalf("negative idle delay=%s want=%s", got, min)
	}
}

func TestParseIndexMetrics(t *testing.T) {
	output := "noise\n" + indexResultPrefix + `{"files_seen":5007,"files_changed":1,"files_queued":1,"messages_added":2,"bytes_read":4096,"db_bytes_delta":8192,"duration_ms":125}` + "\n"
	got, ok := parseIndexMetrics(output)
	if !ok || got.FilesSeen != 5007 || got.FilesChanged != 1 || got.MessagesAdded != 2 || got.BytesRead != 4096 {
		t.Fatalf("metrics=%+v ok=%v", got, ok)
	}
	if _, ok := parseIndexMetrics("missing"); ok {
		t.Fatal("missing metrics reported valid")
	}
}
