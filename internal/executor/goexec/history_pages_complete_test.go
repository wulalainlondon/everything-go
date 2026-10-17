package goexec

import (
	"compress/gzip"
	"everything-go/internal/history"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeHistoryPagesCrossOriginalTailBudgetWithoutGaps(t *testing.T) {
	for _, kind := range []string{"codex", "codex-gzip", "claude"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
			backend := kind
			if kind == "codex-gzip" {
				backend = "codex"
			}
			root := t.TempDir()
			thread := "00000000-0000-0000-0000-000000000981"
			var source strings.Builder
			const rows = 240
			for i := 1; i <= rows; i++ {
				text := fmt.Sprintf("message-%03d-%s", i, strings.Repeat("x", 140))
				if backend == "codex" {
					fmt.Fprintf(&source, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":%q}}\n", text)
				} else {
					fmt.Fprintf(&source, "{\"type\":\"user\",\"message\":{\"content\":%q}}\n", text)
				}
			}
			var provider history.Provider
			if backend == "codex" {
				p := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
				if kind == "codex-gzip" {
					p += ".gz"
					f, e := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0600)
					if e != nil {
						t.Fatal(e)
					}
					z := gzip.NewWriter(f)
					z.Write([]byte(source.String()))
					z.Close()
					f.Close()
				} else {
					if err := os.WriteFile(p, []byte(source.String()), 0600); err != nil {
						t.Fatal(err)
					}
				}
				c := NewCodex(&capSink{}, "codex")
				c.sessionsRoot = root
				c.SetDataDir(t.TempDir())
				provider = c
			} else {
				dir := filepath.Join(root, "project")
				os.Mkdir(dir, 0700)
				os.WriteFile(filepath.Join(dir, thread+".jsonl"), []byte(source.String()), 0600)
				c := NewClaude(&capSink{}, "claude")
				c.projectsDir = root
				provider = c
			}
			page, err := provider.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 12, IncludeThinking: true})
			if err != nil {
				t.Fatal(err)
			}
			all := append([]map[string]any(nil), page.Messages...)
			for n := 0; page.HasMoreBefore && n < rows; n++ {
				if len(page.Messages) == 0 {
					t.Fatal("empty page falsely says more")
				}
				cursor := page.Messages[0]["source_message_id"].(string)
				page, err = provider.LoadHistory(thread, history.Opts{Mode: "snapshot", Before: cursor, Limit: 12, IncludeThinking: true})
				if err != nil {
					t.Fatal(err)
				}
				if !page.KnownIDFound {
					t.Fatalf("cursor fell outside tail budget: %s", cursor)
				}
				all = append(append([]map[string]any(nil), page.Messages...), all...)
			}
			if page.HasMoreBefore || len(all) != rows {
				t.Fatalf("incomplete history: got%d/%d more=%v", len(all), rows, page.HasMoreBefore)
			}
			for i, m := range all {
				want := fmt.Sprintf("%s:%s:line:%d", backend, thread, i+1)
				if m["source_message_id"] != want {
					t.Fatalf("row%d source=%v want%s", i, m["source_message_id"], want)
				}
			}
		})
	}
}

func TestCodexHistoryWindowPagesKeepCommentaryAndTurnOwnershipAtEdges(t *testing.T) {
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000982"
	var raw strings.Builder
	for turn := 1; turn <= 30; turn++ {
		fmt.Fprintf(&raw, "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"turn-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"human-%d\"}}\n", turn)
		for j := 1; j <= 3; j++ {
			fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":%q}}\n", fmt.Sprintf("progress-%d-%d %s", turn, j, strings.Repeat("x", 200+(turn%5)*400)))
		}
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call-%d\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-%d\",\"output\":%q}}\n", turn, "tool-output"+strings.Repeat("o", 1100))
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"final-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-%d\"}}\n", turn)
	}
	path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	os.WriteFile(path, []byte(raw.String()), 0600)
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.SetDataDir(t.TempDir())
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "1048576")
	full, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 1000, IncludeThinking: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Messages) != 90 {
		t.Fatalf("fixture must include30tool rows: got%d", len(full.Messages))
	}
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
	anchored, err := c.LoadHistory(thread, history.Opts{Mode: "continuation", KnownLast: full.Messages[1]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(anchored.Messages) == 0 || anchored.Messages[0]["content_hash"] != full.Messages[1]["content_hash"] {
		t.Fatal("cursor tool/commentary body changed at lookahead edge")
	}
	page, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	all := append([]map[string]any(nil), page.Messages...)
	for n := 0; page.HasMoreBefore && n < 100; n++ {
		if len(page.Messages) == 0 {
			t.Fatal("no progress")
		}
		page, err = c.LoadHistory(thread, history.Opts{Mode: "snapshot", Before: page.Messages[0]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
		if err != nil {
			t.Fatal(err)
		}
		if !page.KnownIDFound {
			t.Fatal("lost cursor")
		}
		all = append(append([]map[string]any(nil), page.Messages...), all...)
	}
	if len(all) != len(full.Messages) {
		t.Fatalf("count %d !=%d", len(all), len(full.Messages))
	}
	for i, m := range all {
		expected := full.Messages[i]
		if m["source_message_id"] != expected["source_message_id"] || m["content_hash"] != expected["content_hash"] || m["source_turn_id"] != expected["source_turn_id"] {
			t.Fatalf("row%d got source=%v hash=%v turn=%v;expected source=%v hash=%v turn=%v", i, m["source_message_id"], m["content_hash"], m["source_turn_id"], expected["source_message_id"], expected["content_hash"], expected["source_turn_id"])
		}
	}
}
