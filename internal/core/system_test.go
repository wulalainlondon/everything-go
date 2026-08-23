package core

import (
	"encoding/json"
	"testing"
	"time"
)

// collectAppends drains sessions_list_append frames until one with done=true,
// or fails on timeout. Returns them in arrival order.
func collectAppends(t *testing.T, c *Client) []map[string]any {
	t.Helper()
	var out []map[string]any
	deadline := time.After(3 * time.Second)
	for {
		select {
		case data := <-c.send:
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatalf("bad event JSON: %v", err)
			}
			if m["type"] != "sessions_list_append" {
				continue
			}
			out = append(out, m)
			if done, _ := m["done"].(bool); done {
				return out
			}
		case <-deadline:
			t.Fatalf("timed out collecting sessions_list_append (got %d so far)", len(out))
		}
	}
}

func TestGetAllSessionsSingleBatch(t *testing.T) {
	h, _ := newTestHub(t)
	for i := 0; i < 3; i++ {
		h.registry.Create("s"+string(rune('0'+i)), "S", t.TempDir(), "claude", "", "", "")
	}
	c := newTestClient(h)
	route(h, c, `{"type":"get_all_sessions"}`)

	batches := collectAppends(t, c)
	if len(batches) != 1 {
		t.Fatalf("want 1 batch for 3 sessions, got %d", len(batches))
	}
	b := batches[0]
	if b["offset"].(float64) != 0 || b["total"].(float64) != 3 || b["done"] != true {
		t.Fatalf("batch envelope wrong: %+v", b)
	}
	if sl, _ := b["sessions"].([]any); len(sl) != 3 {
		t.Fatalf("want 3 sessions in batch, got %d", len(sl))
	}
}

// TestGetAllSessionsBatching: >50 sessions split into 50 + remainder, with
// correct offset/total/done across frames.
func TestGetAllSessionsBatching(t *testing.T) {
	h, _ := newTestHub(t)
	const n = 51
	for i := 0; i < n; i++ {
		h.registry.Create("s"+string(rune(i)), "S", t.TempDir(), "claude", "", "", "")
	}
	c := newTestClient(h)
	route(h, c, `{"type":"get_all_sessions"}`)

	batches := collectAppends(t, c)
	if len(batches) != 2 {
		t.Fatalf("want 2 batches for 51 sessions, got %d", len(batches))
	}
	first, second := batches[0], batches[1]
	if first["offset"].(float64) != 0 || first["done"] != false {
		t.Errorf("first batch wrong: %+v", first)
	}
	if sl, _ := first["sessions"].([]any); len(sl) != 50 {
		t.Errorf("first batch should hold 50, got %d", len(sl))
	}
	if second["offset"].(float64) != 50 || second["total"].(float64) != n || second["done"] != true {
		t.Errorf("second batch wrong: %+v", second)
	}
	if sl, _ := second["sessions"].([]any); len(sl) != 1 {
		t.Errorf("second batch should hold 1, got %d", len(sl))
	}
}

// TestGetAllSessionsEmpty: no sessions → no frame at all (parity with Python's
// empty range loop).
func TestGetAllSessionsEmpty(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"get_all_sessions"}`)

	select {
	case data := <-c.send:
		t.Fatalf("expected no event for an empty session list, got %s", string(data))
	case <-time.After(150 * time.Millisecond):
	}
}

func TestRestartBridgeConfigured(t *testing.T) {
	h, _ := newTestHub(t)
	called := make(chan struct{}, 1)
	h.SetRestart(func() { called <- struct{}{} })

	c := newTestClient(h)
	route(h, c, `{"type":"restart_bridge"}`)

	waitForType(t, c, "restart_ack")
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("restart action was not invoked")
	}
}

func TestRestartBridgeNotConfigured(t *testing.T) {
	h, _ := newTestHub(t) // SetRestart never called → nil
	c := newTestClient(h)
	route(h, c, `{"type":"restart_bridge"}`)

	ev := waitForType(t, c, "error")
	if ev["message"] != "Restart not configured on this bridge" {
		t.Fatalf("error message wrong: %v", ev["message"])
	}
}
