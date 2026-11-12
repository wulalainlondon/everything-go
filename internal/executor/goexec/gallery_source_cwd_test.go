package goexec

import (
	"everything-go/internal/history"
	"testing"
)

func TestNativeHistoryGalleryOriginalCwd(t *testing.T) {
	lines := []history.TailLine{{LineNo: 1, Data: []byte(`{"type":"turn_context","payload":{"turn_id":"t1","cwd":"/original"}}`)}, {LineNo: 2, Data: []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"![image](images/a.png)"}]}}`)}}
	rows, _, err := parseCodexHistoryWindow(lines, "physical", false, true, true)
	if err != nil || len(rows) != 1 || rows[0]["source_cwd"] != "/original" {
		t.Fatalf("original cwd lost: %v %v", rows, err)
	}
	rows, _, err = parseCodexHistoryWindow(lines[1:], "physical", true, true, true)
	if err != nil || len(rows) != 1 || rows[0]["source_cwd"] != nil {
		t.Fatal("unobserved cwd guessed")
	}
	raw := []history.TailLine{{LineNo: 1, Data: []byte(`{"type":"assistant","cwd":"/claude/original","message":{"content":"![image](images/a.png)","stop_reason":"end_turn"}}`)}}
	rows, _, err = parseClaudeHistoryLines(raw, "physical", 1, false)
	if err != nil || len(rows) != 1 || rows[0]["source_cwd"] != "/claude/original" {
		t.Fatalf("Claude cwd lost %v %v", rows, err)
	}
}
