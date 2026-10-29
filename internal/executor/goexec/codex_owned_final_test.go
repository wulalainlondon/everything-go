package goexec

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

func ownedFinalFixture(t *testing.T, mode string) (*Codex, *session.Session, *toolTestWriter) {
	t.Helper()
	c, s, _, w, _ := livenessFixture(t)
	c.connectOnly = true
	c.appServerMode = "daemon"
	c.appServerSocket, _ = healthServer(t, func(string, string) (any, bool) { return nil, false })
	c.subjectAuthPath = filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(c.subjectAuthPath, []byte(`{"tokens":{"account_id":"original"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EVERYTHING_GO_CODEX_EXACT_FINAL_SUBJECT_SHA256", recovery.Hash([]byte("chatgpt:original")))
	if err := c.rememberAcceptedTurnRequest("root", "turn-1", "request-1"); err != nil {
		t.Fatal(err)
	}
	subjectReads := 0
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var p map[string]any
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		switch method {
		case "account/read":
			return map[string]any{"account": map[string]any{"type": "chatgpt"}}, nil
		case "account/rateLimits/read":
			subjectReads++
			account := "original"
			if mode == "other-subject" || mode == "subject-after-change" && subjectReads == 2 {
				account = "other"
			}
			return map[string]any{"accountId": account, "ordinaryUsageAllowed": false}, nil
		case "thread/turns/list":
			if p["threadId"] != "root" || p["itemsView"] != "full" || p["limit"] != float64(1) {
				t.Fatal(p)
			}
			id, status := "turn-1", "completed"
			if mode == "wrong-turn" {
				id = "turn-other"
			}
			if mode == "failed" {
				status = "failed"
			}
			return map[string]any{"data": []any{map[string]any{"id": id, "status": status}}}, nil
		case "thread/items/list":
			if p["threadId"] != "root" || p["turnId"] != "turn-1" || p["sortDirection"] != "asc" || p["limit"] != float64(100) {
				t.Fatal(p)
			}
			if mode == "unknown" {
				return nil, errors.New("unknown original read")
			}
			phase := "final_answer"
			if mode == "commentary" {
				phase = "commentary"
			}
			turn := "turn-1"
			if mode == "other-item-turn" {
				turn = "other"
			}
			item := map[string]any{"turnId": turn, "item": map[string]any{"type": "agentMessage", "id": "final-native-item", "phase": phase, "text": "exact public final"}}
			data := []any{item}
			var next any
			if mode == "duplicate" {
				data = append(data, item)
			}
			if mode == "two-finals" {
				data = append(data, map[string]any{"turnId": turn, "item": map[string]any{"type": "agentMessage", "id": "second-final", "phase": "final_answer", "text": "other"}})
			}
			if mode == "cursor-loop" {
				next = "repeat"
				data = []any{}
			}
			if mode == "paged" && p["cursor"] == nil {
				data = []any{map[string]any{"turnId": turn, "item": map[string]any{"type": "agentMessage", "id": "progress", "phase": "commentary", "text": "progress not final"}}}
				next = "second"
			}
			if mode == "epoch-change" {
				c.rpc.transportEpoch.Add(1)
			}
			if mode == "config-change" {
				s.SetModel("different-fixture-model")
			}
			if mode == "missing-cursor" {
				return map[string]any{"data": data}, nil
			}
			return map[string]any{"data": data, "nextCursor": next}, nil
		}
		t.Fatal("unexpected non-read RPC", method)
		return nil, nil
	}
	return c, s, w
}

func TestOwnedFinalExactPublicCompletedPagesAndCache(t *testing.T) {
	for _, mode := range []string{"single", "paged"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := ownedFinalFixture(t, mode)
			got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if err != nil || got == nil || got.Text != "exact public final" || got.SourceMessageID != "codex:root:item:final-native-item" || got.SubjectSHA256 != recovery.Hash([]byte("chatgpt:original")) || got.RequestID != "request-1" {
				t.Fatal(got, err)
			}
			n := len(w.methods)
			if mode == "single" && n != 5 || mode == "paged" && n != 6 {
				t.Fatal(w.methods)
			}
			got.Text = "modified caller copy"
			got.ItemPageSHA256[0] = "modified"
			again, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if err != nil || again.Text != "exact public final" || again.ItemPageSHA256[0] == "modified" || len(w.methods) != n {
				t.Fatal(again, err, w.methods)
			}
		})
	}
}

func TestOwnedFinalRejectsUnknownSourceTerminalAndIncompleteProof(t *testing.T) {
	for _, mode := range []string{"missing-cursor", "subject-after-change", "other-subject", "wrong-turn", "failed", "commentary", "other-item-turn", "duplicate", "two-finals", "cursor-loop", "unknown", "epoch-change", "config-change"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := ownedFinalFixture(t, mode)
			got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if err == nil || got != nil {
				t.Fatal("unproven final exposed", got, err)
			}
			n := len(w.methods)
			if mode != "epoch-change" && mode != "config-change" {
				got, err = c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
				if err == nil || got != nil || len(w.methods) != n {
					t.Fatal("failed read was polled again", w.methods)
				}
			}
			for _, m := range w.methods {
				if m != "account/read" && m != "account/rateLimits/read" && m != "thread/turns/list" && m != "thread/items/list" {
					t.Fatal(m)
				}
			}
		})
	}
}

func TestOwnedFinalRequiresOptInOriginalSubjectAndDurableMap(t *testing.T) {
	for _, mode := range []string{"disabled", "managed", "unmapped", "local-subject-changed", "wrong-thread"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := ownedFinalFixture(t, "single")
			thread := "root"
			switch mode {
			case "disabled":
				t.Setenv("EVERYTHING_GO_CODEX_EXACT_FINAL_SUBJECT_SHA256", "")
			case "managed":
				c.connectOnly = false
			case "unmapped":
				os.Remove(c.turnRequestsPath("root"))
			case "local-subject-changed":
				os.WriteFile(c.subjectAuthPath, []byte(`{"tokens":{"account_id":"other"}}`), 0600)
			case "wrong-thread":
				thread = "other"
			}
			got, err := c.ExactProviderFinalForRequest(s, "request-1", thread, "turn-1")
			if got != nil || len(w.methods) != 0 {
				t.Fatal(got, err, w.methods)
			}
			if mode != "disabled" && mode != "managed" && err == nil {
				t.Fatal("unguarded source")
			}
		})
	}
}

func TestOwnedFinalKeepsHistoryCatalogIsolated(t *testing.T) {
	c, s, _ := ownedFinalFixture(t, "single")
	c.sessionsRoot = t.TempDir()
	before := c.sessionsRoot
	if _, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(before)
	if err != nil || len(entries) != 0 || c.sessionsRoot != before {
		t.Fatal(entries, err)
	}
	if ids, err := c.ResumableSessions(10); err != nil || !reflect.DeepEqual(len(ids), 0) {
		t.Fatal(ids, err)
	}
}
