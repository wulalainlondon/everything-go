package core

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestTaskUnifiedDisclosureAfterAwaitRejectsGrantAndConfigABA(t *testing.T) {
	for _, caseName := range []string{"get/grant_revoke", "get/grant_ABA", "get/config_ABA", "get/thread_ABA", "read_result/grant_revoke", "read_document/grant_ABA"} {
		t.Run(caseName, func(t *testing.T) {
			h, call, a, b := unifiedFixtureSessions(t)
			loc := seedUnifiedController(t, h, a, b, "disclosure-original")
			manager := h.unifiedIdentity(a.ID)
			op := strings.Split(caseName, "/")[0]
			change := strings.Split(caseName, "/")[1]
			completeJarvisAssignmentFixture(t, h, jarvisAssignment{IntentID: "r_disclosure-original", Target: h.unifiedIdentity(b.ID)}, "disclosure-seal")
			input := map[string]any{"locator": loc}
			if op == "read_result" {
				input["owner_request_id"] = "r_disclosure-original"
				input["seal_id"] = "disclosure-seal"
				input["expected_hash"] = jarvisTextHash("result")
				input["offset"] = 0
				input["limit"] = 32768
			}
			if op == "read_document" {
				body := []byte("immutable document")
				os.WriteFile(filepath.Join(h.cfg.RootDir, "qa-handoff.txt"), body, 0600)
				r, _ := unifiedHTTP(t, call, manager, "document_collect", "disclosure-collect", map[string]any{"locator": loc, "owner_request_id": "r_disclosure-original", "seal_id": "disclosure-seal", "path": "qa-handoff.txt", "expected_hash": taskapi.RawDigest(body)})
				id := unifiedResult(t, r)["data"].(map[string]any)["document_id"]
				input["document_id"] = id
				input["expected_hash"] = taskapi.RawDigest(body)
				input["offset"] = 0
				input["limit"] = 32768
			}
			r, _ := unifiedHTTP(t, call, manager, op, "", input)
			unifiedResult(t, r)
			held, release := make(chan struct{}), make(chan struct{})
			txDone := make(chan error, 1)
			go func() {
				txDone <- h.messageQueue.TaskJournal().Transaction(t.Context(), func(*sql.Tx) error { close(held); <-release; return nil })
			}()
			<-held
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
				<-txDone
			}()
			done := make(chan map[string]any, 1)
			go func() { r, _ := unifiedHTTP(t, call, manager, op, "", input); done <- r }()
			blocked := false
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
					if strings.Contains(stack, "core.(*Hub).resolveUnified") && strings.Contains(stack, "messagequeue.(*Store).EffectiveInput") && strings.Contains(stack, "database/sql.(*DB).conn") {
						blocked = true
						break
					}
				}
				if blocked {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !blocked {
				t.Fatal("did not reach actual post-authorize original queue await")
			}
			switch change {
			case "grant_revoke", "grant_ABA":
				g, e := h.dispatches.Grant(t.Context(), a.ID)
				if e != nil {
					t.Fatal(e)
				}
				old := g
				g, e = h.dispatches.SetGrant(t.Context(), a.ID, sessiondispatch.Grant{Enabled: false, Local: true}, g.Revision)
				if e != nil {
					t.Fatal(e)
				}
				if change == "grant_ABA" {
					if _, e = h.dispatches.SetGrant(t.Context(), a.ID, old, g.Revision); e != nil {
						t.Fatal(e)
					}
				}
			case "config_ABA":
				rev := b.SettingsSnapshot().ConfigRevision
				b.SetConfigRevision(rev + 1)
				b.SetConfigRevision(rev)
			case "thread_ABA":
				thread := b.ResumeID()
				b.SetResumeID("temporary-other-thread")
				b.SetResumeID(thread)
			}
			close(release)
			select {
			case r := <-done:
				if r["ok"] != false {
					t.Fatalf("revoked/ABA source disclosed %#v", r)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read stuck")
			}
		})
	}
}
func TestTaskUnifiedNativeOwnerDocumentIssuanceAndConsumerSubset(t *testing.T) {
	h, _, a, b := unifiedFixtureSessions(t)
	loc := seedUnifiedController(t, h, a, b, "native-doc")
	completeJarvisAssignmentFixture(t, h, jarvisAssignment{IntentID: "r_native-doc", Target: h.unifiedIdentity(b.ID)}, "native-doc-seal")
	owner := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + a.ID, SourceSessionID: a.ID, SourceResumeID: a.ResumeID(), SourceConfigRevision: a.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool"}
	body := []byte("native handoff bytes")
	os.WriteFile(filepath.Join(h.cfg.RootDir, "native-handoff.txt"), body, 0600)
	invoke := func(c taskapi.VerifiedContext, manager, op, key string, in unifiedInput) (any, error) {
		input, _ := json.Marshal(in)
		return h.unifiedDocument(t.Context(), c, manager, taskapi.Request{Operation: op, IdempotencyKey: key, Input: input}, in, func(_ context.Context) error { return nil })
	}
	collected, e := invoke(owner, a.ID, "document_collect", "native-collect", unifiedInput{Locator: loc, OwnerRequest: "r_native-doc", SealID: "native-doc-seal", Path: "native-handoff.txt", Hash: taskapi.RawDigest(body)})
	if e != nil {
		t.Fatal(e)
	}
	var doc struct {
		ID string `json:"document_id"`
	}
	json.Unmarshal(collected.(taskapi.MutationReceipt).Data, &doc)
	granted, e := invoke(owner, a.ID, "document_grant", "native-grant", unifiedInput{Locator: loc, DocumentID: doc.ID, Consumer: h.unifiedIdentity(b.ID), Purpose: "read-only", Expires: time.Now().Add(time.Hour).UnixMilli()})
	if e != nil {
		t.Fatal("original native owner issuance", e)
	}
	var grant struct {
		ID string `json:"grant_id"`
	}
	json.Unmarshal(granted.(taskapi.MutationReceipt).Data, &grant)
	consumer := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + b.ID, SourceSessionID: b.ID, SourceResumeID: b.ResumeID(), SourceConfigRevision: b.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool"}
	in := unifiedInput{Locator: loc, DocumentID: doc.ID, GrantID: grant.ID, Hash: taskapi.RawDigest(body), Limit: 32768}
	read, e := invoke(consumer, b.ID, "read_document", "", in)
	if e != nil || read.(map[string]any)["data_base64"] != base64.StdEncoding.EncodeToString(body) {
		t.Fatal("exact consumer", read, e)
	}
	if _, e = invoke(owner, a.ID, "read_document", "", in); e == nil {
		t.Fatal("native owner issuance became consumer identity")
	}
	wrong := consumer
	wrong.StableScopeID = "session:other"
	wrong.SourceSessionID = "other"
	if _, e = invoke(wrong, b.ID, "read_document", "", in); e == nil {
		t.Fatal("foreign consumer")
	}
	if _, e = invoke(owner, a.ID, "document_revoke", "native-revoke", unifiedInput{Locator: loc, GrantID: grant.ID}); e != nil {
		t.Fatal(e)
	}
	if _, e = invoke(consumer, b.ID, "read_document", "", in); e == nil {
		t.Fatal("revoked consumer")
	}
}

func TestTaskUnifiedQueueSnapshotRequiresRoleAndPinsAwaitGeneration(t *testing.T) {
	for _, kind := range []string{"session_input", "controller_waiting_admission", "delegation_waiting"} {
		t.Run(kind, func(t *testing.T) {
			h, call, a, b := unifiedFixtureSessions(t)
			seedUnifiedController(t, h, a, b, "queue-role")
			target := h.unifiedIdentity(b.ID)
			if kind == "delegation_waiting" {
				target = h.unifiedIdentity(a.ID)
			}
			input := map[string]any{"queue": unifiedQueue{kind, target}}
			r, _ := unifiedHTTP(t, call, h.unifiedIdentity(a.ID), "queue_snapshot", "", input)
			unifiedResult(t, r) // target B grant must allow cross-parent board, not owner-only.
			g, err := h.dispatches.Grant(t.Context(), a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.dispatches.SetGrant(t.Context(), a.ID, sessiondispatch.Grant{Enabled: false, Local: true}, g.Revision); err != nil {
				t.Fatal(err)
			}
			r, _ = unifiedHTTP(t, call, h.unifiedIdentity(a.ID), "queue_snapshot", "", input)
			if r["ok"] != false {
				t.Fatalf("disabled role disclosed even opaque queue: %#v", r)
			}
		})
	}
	for _, change := range []string{"grant_revoke", "grant_ABA", "target_ABA"} {
		t.Run("await/"+change, func(t *testing.T) {
			h, call, a, b := unifiedFixtureSessions(t)
			input := map[string]any{"queue": unifiedQueue{"session_input", h.unifiedIdentity(b.ID)}}
			held, release := make(chan struct{}), make(chan struct{})
			txDone := make(chan error, 1)
			go func() {
				txDone <- h.messageQueue.TaskJournal().Transaction(t.Context(), func(*sql.Tx) error { close(held); <-release; return nil })
			}()
			<-held
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
				<-txDone
			}()
			done := make(chan map[string]any, 1)
			go func() { r, _ := unifiedHTTP(t, call, h.unifiedIdentity(a.ID), "queue_snapshot", "", input); done <- r }()
			blocked := false
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
					if strings.Contains(stack, "core.(*Hub).unifiedQueueView") && strings.Contains(stack, "messagequeue.(*Store).UnifiedQueue") && strings.Contains(stack, "database/sql.(*DB).conn") {
						blocked = true
						break
					}
				}
				if blocked {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !blocked {
				t.Fatal("not blocked at original queue read after initial role check")
			}
			if change == "target_ABA" {
				old := b.ResumeID()
				b.SetResumeID("temporary")
				b.SetResumeID(old)
			} else {
				g, err := h.dispatches.Grant(t.Context(), a.ID)
				if err != nil {
					t.Fatal(err)
				}
				old := g
				g, err = h.dispatches.SetGrant(t.Context(), a.ID, sessiondispatch.Grant{Enabled: false, Local: true}, g.Revision)
				if err != nil {
					t.Fatal(err)
				}
				if change == "grant_ABA" {
					if _, err = h.dispatches.SetGrant(t.Context(), a.ID, old, g.Revision); err != nil {
						t.Fatal(err)
					}
				}
			}
			close(release)
			select {
			case r := <-done:
				if r["ok"] != false {
					t.Fatalf("post-await role or identity change disclosed queue: %#v", r)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("snapshot stuck")
			}
		})
	}
}
