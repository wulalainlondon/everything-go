package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/automation"
	"everything-go/internal/eventinbox"
	"everything-go/internal/session"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInboxHTTPRequiresExactPairedDeviceAndFrozenTarget(t *testing.T) {
	h, _ := newTestHub(t)
	store := attachAutomationStore(t, h, t.TempDir())
	events, err := eventinbox.Open(t.TempDir(), h.cfg.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	h.events = events
	if err = h.pairing.Claim("token", "phone"); err != nil {
		t.Fatal(err)
	}
	h.registry.Create("s_event", "Event", h.cfg.RootDir, "codex", "", "read-only", "")
	e, _, err := events.Insert(context.Background(), eventinbox.Input{Source: "fixture", EventKey: "one", Kind: "updated", Title: "Test event"})
	if err != nil {
		t.Fatal(err)
	}
	request := func(path, body, device string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/automation/v1/"+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer token")
		r.Header.Set("X-Bridge-Device-ID", device)
		w := httptest.NewRecorder()
		h.serveInboxAPI(w, r, path)
		return w
	}
	s, _ := h.registry.Get("s_event")
	a := automation.EventAssignment{ID: "r_exact_event", EventIDs: []string{e.ID}, SessionID: s.ID, OwnerInstanceID: h.cfg.InstanceID, ThreadID: s.ResumeID(), ConfigRevision: s.SettingsSnapshot().ConfigRevision, Mode: "analyze"}
	raw, _ := json.Marshal(a)
	if w := request("assignments", string(raw), "foreign"); w.Code != 403 {
		t.Fatalf("foreign status %d", w.Code)
	}
	bad := a
	bad.ThreadID = "foreign"
	rawBad, _ := json.Marshal(bad)
	if w := request("assignments", string(rawBad), "phone"); w.Code != 409 {
		t.Fatalf("changed status %d body %s", w.Code, w.Body.String())
	}
	if w := request("assignments", string(raw), "phone"); w.Code != 202 {
		t.Fatalf("accept status %d body %s", w.Code, w.Body.String())
	}
	if w := request("assignments", string(raw), "phone"); w.Code != 202 {
		t.Fatalf("original receipt status %d body %s", w.Code, w.Body.String())
	}
	rows, err := store.EventAssignments(context.Background())
	if err != nil || len(rows) != 1 || rows[0].OwnerDevice != "phone" {
		t.Fatalf("receipts %+v %v", rows, err)
	}
	if !strings.Contains(rows[0].Content, "[Untrusted external event data]") {
		t.Fatal("event content is not separated")
	}
}

func TestInboxOutboxDoesNotSubmitAcceptedIntentTwice(t *testing.T) {
	h, exec := newTestHub(t)
	store := attachAutomationStore(t, h, t.TempDir())
	if err := h.pairing.Claim("token", "phone"); err != nil {
		t.Fatal(err)
	}
	s := h.registry.Create("event-target", "Event", h.cfg.RootDir, "codex", "", "read-only", "")
	a := automation.EventAssignment{ID: "r_durable_event", EventIDs: []string{"event"}, SessionID: s.ID, OwnerInstanceID: h.cfg.InstanceID, ThreadID: s.ResumeID(), ConfigRevision: s.SettingsSnapshot().ConfigRevision, OwnerDevice: "phone", Mode: "analyze", Content: "[Operator policy] analyze only"}
	saved, err := store.SaveEventAssignment(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 2)
	exec.onSend = func(_ *session.Session, request, content string) { received <- request }
	if err = h.enqueueEventAssignment(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-received:
		if id != a.ID {
			t.Fatal("wrong original request")
		}
	case <-time.After(time.Second):
		t.Fatal("original event did not reach executor")
	}
	if err = h.enqueueEventAssignment(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
		t.Fatal("accepted request replayed")
	case <-time.After(40 * time.Millisecond):
	}
}
