package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"everything-go/internal/callbacks"
)

func TestCallbackCompactSyncPaginatesInvitationsWithoutReportBodiesOrVoice(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	for i := 0; i < 3; i++ {
		request := fmt.Sprintf("r_page_%d", i)
		id := h.callbackRecordID(s.ID, h.callbackEpoch(s.ID), request)
		_, err := h.callbacks.Mutate(context.Background(), "page_setup_"+request, "fixture", id, 0, func(r *callbacks.Record, _ bool) error {
			*r = callbacks.Record{Authority: h.cfg.InstanceID, Session: s.ID, Request: request, Enabled: true, Recipient: h.callbackRecipient(c), Thread: s.ResumeID(), Report: &callbacks.Report{ID: "report", Text: strings.Repeat("x", 1<<20)}}
			callbacks.NewInvitation(r, time.Now().UnixMilli(), "fixture-invite")
			r.Invite.State = "ringing"
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < 3; page++ {
		got := callbackAction(t, h, c, callbackInput{Action: "sync", Operation: fmt.Sprintf("callback_compact_%d", page), Native: true, Compact: true, Cursor: cursor, Limit: 1})
		if got["accepted"] != true {
			t.Fatal(got)
		}
		raw, _ := json.Marshal(got)
		if len(raw) > 4096 || got["records"] != nil {
			t.Fatal("compact ACK contains legacy full history", len(raw))
		}
		items, ok := got["invitations"].([]any)
		if !ok || len(items) != 1 {
			t.Fatal(got)
		}
		id := items[0].(map[string]any)["id"].(string)
		if seen[id] {
			t.Fatal("duplicate page")
		}
		seen[id] = true
		cursor, _ = got["next_cursor"].(string)
		if (page < 2) == (cursor == "") {
			t.Fatal("cursor boundary", page, cursor)
		}
	}
	if e.starts.Load() != 0 || e.appends.Load() != 0 {
		t.Fatal("registration acquired voice or report execution")
	}
	bad := callbackAction(t, h, c, callbackInput{Action: "sync", Operation: "callback_invalid_page", Native: true, Compact: true, Limit: 65})
	if bad["accepted"] != false {
		t.Fatal("unbounded compact page accepted")
	}
}
