package goexec

import (
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/history"
)

func TestGeminiHistoryAndResumableSessions(t *testing.T) {
	home := t.TempDir()
	t.Setenv("GEMINI_HOME", home)
	dir := filepath.Join(home, "tmp", "project", "chats")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	chat := `{"sessionId":"gem-1234567890","cwd":"/work","messages":[{"type":"user","parts":[{"text":"Build it"}],"timestamp":"2026-08-23T00:00:00Z"},{"type":"gemini","parts":[{"text":"Done"}]}]}`
	if err := os.WriteFile(filepath.Join(dir, "gem-1234567890.json"), []byte(chat), 0o644); err != nil {
		t.Fatal(err)
	}
	g := NewGemini(&capSink{}, "")
	sessions, err := g.ResumableSessions(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Backend != "gemini" || sessions[0].Name != "Build it" {
		t.Fatalf("sessions = %+v", sessions)
	}
	result, err := g.LoadHistory("gem-1234567890", history.Opts{Limit: 10, Mode: "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || result.Messages[1]["role"] != "assistant" || result.Messages[1]["content"] != "Done" {
		t.Fatalf("history = %+v", result)
	}
}
