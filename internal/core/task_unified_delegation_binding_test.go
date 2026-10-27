package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func initializedUnifiedDelegation(t *testing.T) (*Hub, *session.Session, taskapi.IntentRecord, delegation.Record, func(string, string, any, string) *httptest.ResponseRecorder) {
	t.Helper()
	// The original create/provision fixture starts with an empty execution thread.
	// Its provider Send initializes that thread and emits original correlated
	// acceptance through Hub. The test never seeds a known execution expectation.
	h, parent, _, intent, d := extensionFormal(t)
	initial, rev, _, e := h.delegations.ExecutionBinding(t.Context(), d.ID)
	if e != nil || initial != "" {
		t.Fatal("initial expectation must stay empty", initial, rev, e)
	}
	child, ok := h.registry.Get(d.ChildSessionID)
	if !ok {
		t.Fatal("missing original provisioned child")
	}
	pin, pinRev, found, e := h.delegations.NativeThread(t.Context(), child.ID)
	if e != nil || !found || pin == "" || pin != child.ResumeID() || pinRev != rev {
		t.Fatal("missing original native initialization pin", pin, pinRev, found, e)
	}
	native, accepted, e := h.messageQueue.ExactNativeAcceptance(child.ID, d.ChildRequestID)
	if e != nil || !accepted || native.ThreadID != pin || !h.nativeCorrelationKnown(child, native.ThreadID, d.ChildRequestID, native.TurnID) {
		t.Fatal("missing same-round provider proof", native, accepted, e)
	}
	if e = h.pairing.Claim("initialized-owned-paired-token", "phone"); e != nil {
		t.Fatal(e)
	}
	call := func(method, path string, input any, device string) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(input)
		r := httptest.NewRequest(method, "/api/jarvis/v1/"+path, strings.NewReader(string(raw)))
		r.Header.Set("Authorization", "Bearer initialized-owned-paired-token")
		r.Header.Set("X-Bridge-Device-ID", device)
		w := httptest.NewRecorder()
		h.ServeJarvisAPI(w, r)
		return w
	}
	return h, parent, intent, d, call
}

func TestTaskUnifiedFreshDelegationNativeInitializationReadsOriginal(t *testing.T) {
	h, parent, intent, d, call := initializedUnifiedDelegation(t)
	child, _ := h.registry.Get(d.ChildSessionID)
	manager := h.unifiedIdentity(parent.ID)
	if dir := os.Getenv("TASK_UNIFIED_INITIALIZED_HTTP_FIXTURE_DIR"); dir != "" {
		if e := os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		entries := []map[string]any{}
		originalCall := call
		write := func(name string, raw []byte) {
			t.Helper()
			f, e := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = f.Write(raw); e != nil {
				t.Fatal(e)
			}
			if e = f.Close(); e != nil {
				t.Fatal(e)
			}
		}
		call = func(method, path string, input any, device string) *httptest.ResponseRecorder {
			request, _ := json.Marshal(input)
			w := originalCall(method, path, input, device)
			var envelope map[string]any
			json.Unmarshal(request, &envelope)
			op, _ := envelope["operation"].(string)
			name := fmt.Sprintf("%02d-%s", len(entries)+1, op)
			response := append([]byte(nil), w.Body.Bytes()...)
			write(name+".request.json", request)
			write(name+".response.json", response)
			entries = append(entries, map[string]any{"name": name, "operation": op, "request_path": name + ".request.json", "request_sha256": taskapi.RawDigest(request), "request_bytes": len(request), "relative_path": name + ".response.json", "sha256": taskapi.RawDigest(response), "bytes": len(response), "response_http_status": w.Code, "expected_schema_valid": true})
			return w
		}
		defer func() {
			raw, _ := json.MarshalIndent(map[string]any{"source_commit": os.Getenv("TASK_UNIFIED_SOURCE_COMMIT"), "source_tree_hash": os.Getenv("TASK_UNIFIED_SOURCE_TREE_HASH"), "schema_hash": "695b0aa0063770b48e8fa9f17e7484a6372a6504c21e791da8824181612da847", "raw_test_refs": []string{os.Getenv("TASK_UNIFIED_RAW_LOG"), os.Getenv("TASK_UNIFIED_RAW_RECEIPT")}, "scope": "Isolated original newworker create/provision→first synthetic provider native acceptance/pin/seal→actual paired Hub HTTP; zero live/provider/model/device effects. Original admission expected thread stays empty.", "entries": entries}, "", "  ")
			write("manifest.json", raw)
		}()
	}
	caps, _ := unifiedHTTP(t, call, manager, "capabilities", "", nil)
	unifiedResult(t, caps)
	selectors := []map[string]any{{"kind": "delegation", "authority_instance_id": h.cfg.InstanceID, "native_id": d.ID}, {"kind": "task", "authority_instance_id": h.cfg.InstanceID, "task_id": intent.TaskID, "thread_id": child.ResumeID(), "config_revision": child.SettingsSnapshot().ConfigRevision}}
	for _, loc := range selectors {
		for _, op := range []string{"resolve", "get"} {
			response, _ := unifiedHTTP(t, call, manager, op, "", map[string]any{"locator": loc})
			value := unifiedResult(t, response)
			owner := value["owner"].(map[string]any)
			if owner["thread_id"] != child.ResumeID() || owner["config_revision"] != float64(0) {
				t.Fatal("wrong derived original target", owner)
			}
		}
	}
	legacy, e := h.apiTask(t.Context(), intent)
	if e != nil {
		t.Fatal(e)
	}
	seal := legacy["seal_id"].(string)
	body := []byte("current synthetic handoff; collection-time only")
	if e = os.WriteFile(filepath.Join(child.Cwd(), "handoff.txt"), body, 0600); e != nil {
		t.Fatal(e)
	}
	loc := selectors[1]
	response, _ := unifiedHTTP(t, call, manager, "read_result", "", map[string]any{"locator": loc, "owner_request_id": d.ChildRequestID, "seal_id": seal, "expected_hash": legacy["final_hash"], "offset": 0, "limit": 32768})
	if unifiedResult(t, response)["full_hash"] != legacy["final_hash"] {
		t.Fatal("original sealed result hash changed")
	}
	response, _ = unifiedHTTP(t, call, manager, "document_collect", "initialized-collect", map[string]any{"locator": loc, "owner_request_id": d.ChildRequestID, "seal_id": seal, "path": "handoff.txt", "expected_hash": taskapi.RawDigest(body)})
	doc := unifiedResult(t, response)["data"].(map[string]any)
	response, _ = unifiedHTTP(t, call, manager, "read_document", "", map[string]any{"locator": loc, "document_id": doc["document_id"], "expected_hash": taskapi.RawDigest(body), "offset": 0, "limit": 32768})
	if unifiedResult(t, response)["data_base64"] != base64.StdEncoding.EncodeToString(body) {
		t.Fatal("immutable original document read changed")
	}
	consumer, ok := h.registry.Get("target")
	if !ok {
		t.Fatal("original isolated consumer missing")
	}
	response, _ = unifiedHTTP(t, call, manager, "document_grant", "initialized-grant", map[string]any{"locator": loc, "document_id": doc["document_id"], "consumer": h.unifiedIdentity(consumer.ID), "purpose": "read synthetic immutable snapshot only", "expires_at": time.Now().Add(time.Minute).UnixMilli()})
	grant := unifiedResult(t, response)["data"].(map[string]any)
	response, _ = unifiedHTTP(t, call, manager, "document_revoke", "initialized-revoke", map[string]any{"locator": loc, "grant_id": grant["grant_id"]})
	if unifiedResult(t, response)["data"].(map[string]any)["revoked"] != true {
		t.Fatal("original grant not revoked")
	}
	response, _ = unifiedHTTP(t, call, h.unifiedIdentity(consumer.ID), "get", "", map[string]any{"locator": map[string]any{"kind": "delegation", "authority_instance_id": h.cfg.InstanceID, "native_id": d.ID}})
	if response["ok"] != false {
		t.Fatal("unrelated manager read another original relation", response)
	}
	thread, rev, _, e := h.delegations.ExecutionBinding(t.Context(), d.ID)
	if e != nil || thread != "" || rev != 0 {
		t.Fatal("read rewrote original admission binding", thread, rev, e)
	}
	for _, wrong := range []map[string]any{{"kind": "task", "authority_instance_id": h.cfg.InstanceID, "task_id": intent.TaskID, "thread_id": "foreign-thread", "config_revision": 0}, {"kind": "task", "authority_instance_id": h.cfg.InstanceID, "task_id": intent.TaskID, "thread_id": child.ResumeID(), "config_revision": 1}} {
		response, _ = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": wrong})
		if response["ok"] != false {
			t.Fatal("foreign selector adopted", response)
		}
	}
}

func TestTaskUnifiedFreshDelegationInitializationRefusesUnboundOrChanged(t *testing.T) {
	for _, change := range []string{"missing_pin", "foreign_pin", "pin_revision", "missing_acceptance", "native_conflict", "provider_unbound", "legacy_profile", "thread", "config", "hidden", "closed"} {
		t.Run(change, func(t *testing.T) {
			h, parent, _, d, call := initializedUnifiedDelegation(t)
			child, _ := h.registry.Get(d.ChildSessionID)
			alter := func(sqlText string, args ...any) {
				t.Helper()
				if e := h.delegations.TaskJournal().Transaction(t.Context(), func(tx *sql.Tx) error { _, e := tx.ExecContext(t.Context(), sqlText, args...); return e }); e != nil {
					t.Fatal(e)
				}
			}
			switch change {
			case "missing_pin":
				alter("DELETE FROM delegation_native_threads WHERE child=?", child.ID)
			case "foreign_pin":
				alter("UPDATE delegation_native_threads SET thread='foreign-thread' WHERE child=?", child.ID)
			case "pin_revision":
				alter("UPDATE delegation_native_threads SET revision=revision+1 WHERE child=?", child.ID)
			case "legacy_profile":
				alter("DELETE FROM task_api_child_profiles WHERE session_id=?", child.ID)
			case "missing_acceptance":
				if e := h.messageQueue.TaskJournal().Transaction(t.Context(), func(tx *sql.Tx) error {
					_, e := tx.ExecContext(t.Context(), "DELETE FROM task_native_acceptance WHERE session_id=? AND request_id=?", child.ID, d.ChildRequestID)
					return e
				}); e != nil {
					t.Fatal(e)
				}
			case "native_conflict":
				if e := h.messageQueue.RecordNativeAcceptance(child.ID, d.ChildRequestID, child.ResumeID(), "conflicting-turn"); e != messagequeue.ErrNativeConflict {
					t.Fatal(e)
				}
			case "provider_unbound":
				p, ok := h.exec.(historyRouter).ProviderFor(child)
				if !ok {
					t.Fatal("provider fixture missing")
				}
				v := p.(*correlationHistoryFixture)
				v.mu.Lock()
				v.Owners = map[string]string{}
				v.mu.Unlock()
			case "thread":
				child.SetResumeID("foreign-thread")
			case "config":
				child.SetConfigRevision(1)
			case "hidden":
				hidden := true
				child.SetMeta(nil, &hidden)
			case "closed":
				child.CloseAndWait()
			}
			response, _ := unifiedHTTP(t, call, h.unifiedIdentity(parent.ID), "get", "", map[string]any{"locator": map[string]any{"kind": "delegation", "authority_instance_id": h.cfg.InstanceID, "native_id": d.ID}})
			if response["ok"] != false {
				t.Fatal("unbound/changed original initialization disclosed", change, response)
			}
		})
	}
}

func TestTaskUnifiedFreshDelegationInitializationDisclosureRejectsABA(t *testing.T) {
	for _, change := range []string{"child_thread", "child_config", "parent_thread", "parent_config"} {
		t.Run(change, func(t *testing.T) {
			h, parent, _, d, _ := initializedUnifiedDelegation(t)
			child, _ := h.registry.Get(d.ChildSessionID)
			caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", BindingKind: "paired_human"}
			resolved, e := h.resolveUnified(t.Context(), caller, parent.ID, unifiedLocator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: d.ID})
			if e != nil {
				t.Fatal(e)
			}
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
			done := make(chan error, 1)
			go func() { _, e := h.unifiedTaskDTO(context.Background(), caller, parent.ID, resolved); done <- e }()
			blocked := false
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
					if strings.Contains(stack, "core.(*Hub).unifiedTaskDTO") && strings.Contains(stack, "messagequeue.(*Store).EffectiveInput") && strings.Contains(stack, "database/sql.(*DB).conn") {
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
				t.Fatal("original read did not await original MQ data after initialization fence")
			}
			target := child
			if strings.HasPrefix(change, "parent_") {
				target = parent
			}
			if strings.HasSuffix(change, "thread") {
				old := target.ResumeID()
				target.SetResumeID("ABA-intermediate")
				target.SetResumeID(old)
			} else {
				old := target.SettingsSnapshot().ConfigRevision
				target.SetConfigRevision(old + 1)
				target.SetConfigRevision(old)
			}
			close(release)
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("source/target ABA disclosed after await")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("original read stuck")
			}
		})
	}
}

// Capture original native authority, then let a conflict commit before the MQ
// writer. Final transport refusal cannot undo a grant/reply already persisted.
func TestTaskUnifiedFreshDelegationConflictBeforeWriterHasNoEffect(t *testing.T) {
	for _, op := range []string{"reply", "document_grant"} {
		t.Run(op, func(t *testing.T) {
			h, parent, _, d, _ := initializedUnifiedDelegation(t)
			c := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", BindingKind: "paired_human"}
			r, e := h.resolveUnified(t.Context(), c, parent.ID, unifiedLocator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: d.ID})
			if e != nil {
				t.Fatal(e)
			}
			if e = h.messageQueue.RecordNativeAcceptance(d.ChildSessionID, d.ChildRequestID, r.Target.ThreadID, "late-conflicting-turn"); e != messagequeue.ErrNativeConflict {
				t.Fatal(e)
			}
			m := taskapi.QueueMutation{Scope: "synthetic-initialized-writer", Generation: 1, Intent: "conflict-before-writer", Hash: "original-hash", Operation: op, Revalidate: r.disclosureCurrent}
			if op == "reply" {
				_, e = h.messageQueue.UnifiedReply(t.Context(), m, []byte(`{"synthetic":true}`))
			} else {
				g := messagequeue.UnifiedDocumentGrant{ID: "synthetic-denied-grant", Document: "synthetic-document", OwnerScope: m.Scope, OwnerGeneration: m.Generation, Consumer: []byte(`{}`), Purpose: "synthetic", Expires: time.Now().Add(time.Minute).UnixMilli()}
				_, e = h.messageQueue.UnifiedDocumentMutation(t.Context(), m, nil, &g, "", map[string]any{"granted": true})
				if _, foundErr := h.messageQueue.UnifiedGrant(t.Context(), g.ID); foundErr != sql.ErrNoRows {
					t.Fatal("refused writer retained grant", foundErr)
				}
			}
			if e == nil {
				t.Fatal("conflict committed before writer but effect persisted")
			}
			_, found, lookupErr := h.messageQueue.UnifiedOperation(t.Context(), m)
			if lookupErr != nil || found {
				t.Fatal("refused writer retained mutation", found, lookupErr)
			}
		})
	}
}

func TestTaskUnifiedFreshDelegationProductionReplyConflictBeforeWriter(t *testing.T) {
	for _, delivery := range []string{"metadata", "queue"} {
		t.Run(delivery, func(t *testing.T) {
			h, parent, _, d, _ := initializedUnifiedDelegation(t)
			c := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", BindingKind: "paired_human"}
			sum := sha256.Sum256([]byte("initialized-owned-paired-token"))
			c.NamespaceGeneration = binary.BigEndian.Uint64(sum[:8]) & ((1 << 53) - 1)
			loc := unifiedLocator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: d.ID}
			r, e := h.resolveUnified(t.Context(), c, parent.ID, loc)
			if e != nil {
				t.Fatal(e)
			}
			// A second handle belongs ONLY to this temporary synthetic test database.
			// It commits the exact conflict at the production source-revalidation seam,
			// after resolve but before the first metadata/delegation writer statement.
			db, e := sql.Open("sqlite", filepath.Join(h.cfg.DataDir, "message_queue.sqlite"))
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			injected := false
			check := func(context.Context) error {
				if !injected {
					injected = true
					_, err := db.Exec("INSERT INTO task_native_conflicts(session_id,request_id,thread_id,turn_id) VALUES(?,?,?,?)", d.ChildSessionID, d.ChildRequestID, r.Target.ThreadID, "production-late-conflict")
					return err
				}
				return nil
			}
			req := taskapi.Request{Operation: "reply", IdempotencyKey: "production-conflict-" + delivery}
			in := unifiedInput{Locator: loc, Direction: "parent_to_worker", Source: r.Parent, Target: r.Target, ReplyTo: r.Locator.RequestID, ReplyHash: r.ReplyHash, Content: "synthetic must not execute", Delivery: delivery}
			_, e = h.unifiedReply(t.Context(), c, parent.ID, req, in, check)
			t.Log("production response", delivery, "injected", injected, "error", e)
			if !injected {
				t.Fatal("did not reach production writer revalidation")
			}
			if e == nil {
				t.Fatal("production reply accepted despite conflict before writer")
			}
			m := unifiedMutation(c, parent.ID, req, check)
			for _, lookup := range []func(context.Context, taskapi.QueueMutation) (taskapi.MutationReceipt, bool, error){h.messageQueue.UnifiedOperation, h.delegations.UnifiedOperation} {
				_, found, err := lookup(t.Context(), m)
				if err != nil || found {
					t.Fatal("production refusal retained receipt", found, err)
				}
			}
			latest, err := h.delegations.LatestRound(t.Context(), d.ID)
			if err != nil || latest.ID != d.ID {
				t.Fatal("production refusal created successor", latest.ID, err)
			}
		})
	}
}

func TestTaskUnifiedFreshDelegationProductionReplyPreservesOriginalRound(t *testing.T) {
	h, parent, intent, d, call := initializedUnifiedDelegation(t)
	c := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", BindingKind: "paired_human"}
	sum := sha256.Sum256([]byte("initialized-owned-paired-token"))
	c.NamespaceGeneration = binary.BigEndian.Uint64(sum[:8]) & ((1 << 53) - 1)
	loc := unifiedLocator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: d.ID}
	r, e := h.resolveUnified(t.Context(), c, parent.ID, loc)
	if e != nil {
		t.Fatal(e)
	}
	for _, delivery := range []string{"metadata", "queue"} {
		req := taskapi.Request{Operation: "reply", IdempotencyKey: "production-positive-" + delivery}
		in := unifiedInput{Locator: loc, Direction: "parent_to_worker", Source: r.Parent, Target: r.Target, ReplyTo: r.Locator.RequestID, ReplyHash: r.ReplyHash, Content: "synthetic same work followup", Delivery: delivery}
		value, e := h.unifiedReply(t.Context(), c, parent.ID, req, in, func(context.Context) error { return nil })
		if e != nil {
			t.Fatal(delivery, e)
		}
		receipt := value.(taskapi.MutationReceipt)
		var data map[string]any
		if json.Unmarshal(receipt.Data, &data) != nil {
			t.Fatal(receipt)
		}
		if delivery == "queue" {
			roundID, _ := data["round_id"].(string)
			if roundID == "" || roundID == d.ID || data["logical_task_id"] != d.ID {
				t.Fatal("new round lost original logical identity", data)
			}
			next, found, e := h.delegations.ByID(t.Context(), roundID)
			if e != nil || !found || next.ChildSessionID != d.ChildSessionID {
				t.Fatal("new round target changed", next, e)
			}
			round, found, e := h.delegations.Round(t.Context(), roundID)
			if e != nil || !found || round.PreviousID != d.ID || round.LogicalID != d.ID || round.Version != 2 {
				t.Fatal("new round lost original continuity", round, e)
			}
			for _, op := range []string{"resolve", "get"} {
				response, _ := unifiedHTTP(t, call, h.unifiedIdentity(parent.ID), op, "", map[string]any{"locator": map[string]any{"kind": "delegation", "authority_instance_id": h.cfg.InstanceID, "native_id": roundID, "round_id": roundID}})
				got := unifiedResult(t, response)
				owner := got["owner"].(map[string]any)
				if owner["thread_id"] != r.Target.ThreadID || owner["config_revision"] != float64(0) {
					t.Fatal("next round target not exact", got)
				}
			}
		}
	}
	// The old selected round/task must remain the original sealed request after
	// its successor exists, never implicitly read latest or rewrite admission.
	response, _ := unifiedHTTP(t, call, h.unifiedIdentity(parent.ID), "get", "", map[string]any{"locator": map[string]any{"kind": "task", "authority_instance_id": h.cfg.InstanceID, "task_id": intent.TaskID}})
	got := unifiedResult(t, response)
	legacy, e := h.apiTask(t.Context(), intent)
	if e != nil {
		t.Fatal(e)
	}
	final, ok := got["final"].(map[string]any)
	if !ok || final["owner_request_id"] != d.ChildRequestID || final["seal_id"] != legacy["seal_id"] || final["full_hash"] != legacy["final_hash"] {
		t.Fatal("old selection substituted successor or seal", got)
	}
	locator := got["locator"].(map[string]any)
	if locator["request_id"] != d.ChildRequestID {
		t.Fatal("old locator retargeted", locator)
	}
	initial, rev, _, e := h.delegations.ExecutionBinding(t.Context(), d.ID)
	if e != nil || initial != "" || rev != 0 {
		t.Fatal("original admission rewritten", initial, rev, e)
	}
}
