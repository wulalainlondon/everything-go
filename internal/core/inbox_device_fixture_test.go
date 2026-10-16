package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/automation"
	"everything-go/internal/backend"
	"everything-go/internal/eventinbox"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type inboxDeviceExecutor struct {
	*fakeExec
	hub      *Hub
	mu       sync.Mutex
	question backend.UserInputPayload
	answered atomic.Int32
	calls    atomic.Int32
}

func (f *inboxDeviceExecutor) PendingInteractions(id string) []backend.UserInputPayload {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.question.RequestID != "" && (id == "" || id == "same") {
		return []backend.UserInputPayload{f.question}
	}
	return []backend.UserInputPayload{}
}
func (f *inboxDeviceExecutor) RespondUserInput(id string, _ map[string]any, cancelled bool) bool {
	f.mu.Lock()
	if id != f.question.RequestID || cancelled {
		f.mu.Unlock()
		return false
	}
	f.question = backend.UserInputPayload{}
	f.mu.Unlock()
	f.answered.Add(1)
	f.hub.Emit(map[string]any{"type": "interaction_resolved", "request_id": id, "session_id": "same"})
	return true
}

// Opt-in native-device fixture: real Go HTTP/WebSocket/stores/queue, synthetic provider only.
// It never opens the production data directory or starts a connector scheduler.
func TestInboxNativeDeviceFixture(t *testing.T) {
	if os.Getenv("INBOX_DEVICE_FIXTURE_RUN") != "yes" {
		t.Skip("explicit native-device fixture only")
	}
	log.SetOutput(io.Discard)
	t.Setenv("BRIDGE_AUTH_TOKEN", "synthetic-only")
	output := os.Getenv("INBOX_DEVICE_FIXTURE_REPORT")
	if output == "" {
		t.Fatal("report path required")
	}
	stop := make(chan struct{})
	var stopOnce sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type peer struct {
		h          *Hub
		exec       *inboxDeviceExecutor
		events     []eventinbox.Event
		server     *http.Server
		permission atomic.Bool
		begun      atomic.Bool
		device     atomic.Value
	}
	peers := []*peer{}
	for index, port := range []string{"18766", "18767"} {
		instance := "inbox-native-qa-a"
		if index == 1 {
			instance = "inbox-native-qa-b"
		}
		instance += "-" + os.Getenv("INBOX_DEVICE_FIXTURE_GENERATION")
		dir := t.TempDir()
		dir, _ = filepath.EvalSymlinks(dir)
		reg := session.NewRegistry()
		pairing := governance.NewPairing(filepath.Join(dir, "pairing.json"))
		h := NewHub(reg, Config{InstanceID: instance, InstanceName: instance, DataDir: dir, RootDir: dir}, pairing, 0)
		h.runtimes.Ensure("same", "idle", 0)
		sessionObject := reg.Create("same", "QA 同名對話 A", dir, "codex", "", "read-only", "qa-thread")
		if index == 1 {
			sessionObject.SetName("QA 同名對話 B")
		}
		fe := &fakeExec{sink: h}
		executor := &inboxDeviceExecutor{fakeExec: fe, hub: h}
		h.SetExecutor(executor)
		fe.onSend = func(s *session.Session, id, content string) {
			executor.calls.Add(1)
			h.Emit(protocol.NewTextChunk(s.ID, id, "QA 事件已由隔離 provider 分析；沒有外部操作。"))
			h.Emit(protocol.NewDone(s.ID, id))
		}
		events, err := eventinbox.Open(dir, instance)
		if err != nil {
			t.Fatal(err)
		}
		h.SetEventInbox(events)
		auto, err := automation.Open(dir, instance)
		if err != nil {
			t.Fatal(err)
		}
		h.SetAutomation(auto)
		p := &peer{h: h, exec: executor}
		peers = append(peers, p)
		if index == 0 {
			work, err := workitems.OpenService(dir, instance)
			if err != nil {
				t.Fatal(err)
			}
			h.SetWorkItems(work)
			project, err := work.CreateProject(ctx, workitems.CreateProjectInput{ID: "qa-project", Name: "QA only"})
			if err != nil {
				t.Fatal(err)
			}
			item, err := work.CreateItem(ctx, workitems.CreateItemInput{ID: "qa-work", ProjectID: project.ID, Title: "QA proposal ownership"})
			if err != nil {
				t.Fatal(err)
			}
			item, err = work.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorUser}})
			if err != nil {
				t.Fatal(err)
			}
			_, item, err = work.LinkSession(ctx, workitems.LinkSessionInput{ID: "qa-link", WorkItemID: item.ID, SessionID: "same", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = work.StartRun(ctx, workitems.StartRunInput{ID: "qa-proposal-run", WorkItemID: item.ID, SessionID: "same", RequestID: "r_qa_proposal_run", Kind: "research", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = auto.UpsertAccount(ctx, automation.Account{ID: "qa-account", Provider: "meta.facebook", ExternalAccountID: "synthetic-only", DisplayName: "QA account", CredentialRef: "env:INBOX_FIXTURE_DISABLED", Enabled: true}, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = auto.UpsertRoute(ctx, automation.Route{ID: "qa-proposal-route", Name: "QA ownership only", Enabled: true, AccountID: "qa-account", SourcePattern: "fixture-only", KindPattern: "fixture-only", HandlingMode: automation.ApprovedAction, WorkItemID: item.ID, SessionID: "same", RunKind: "research"}, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = auto.CreateProposal(ctx, automation.ProposalInput{ID: "qa-proposal", AccountID: "qa-account", WorkItemID: item.ID, RunID: "qa-proposal-run", ActionType: "facebook.comment.reply", TargetID: "synthetic-only", Payload: json.RawMessage(`{"message":"人工測試內容，不會對外送出"}`), DisplayPreview: "QA 外部操作草稿"})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"qa-event-1", "qa-event-2"} {
				e, _, err := events.Insert(ctx, eventinbox.Input{Source: "github", EventKey: id, Kind: "issue.updated", Title: "QA 外部事件更新", Body: "這是隔離的收件匣驗收資料。", Data: json.RawMessage(`{"subject_id":42}`)})
				if err != nil {
					t.Fatal(err)
				}
				p.events = append(p.events, e)
			}
			extra, _, err := events.Insert(ctx, eventinbox.Input{Source: "github", EventKey: "qa-new-session-event", Kind: "issue.updated", Title: "QA 新對話事件", Body: "唯讀新對話驗收資料", Data: json.RawMessage(`{"subject_id":43}`)})
			if err != nil {
				t.Fatal(err)
			}
			p.events = append(p.events, extra)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/fixture/enroll", func(w http.ResponseWriter, r *http.Request) {
			var in struct {
				Device string `json:"device_id"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || in.Device == "" {
				http.Error(w, "device required", 400)
				return
			}
			if err := h.pairing.Claim("synthetic-only", in.Device); err != nil {
				http.Error(w, "fixture already bound", 409)
				return
			}
			p.device.Store(in.Device)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instance_id": h.cfg.InstanceID})
		})
		mux.HandleFunc("/fixture/begin", func(w http.ResponseWriter, r *http.Request) {
			if p.begun.CompareAndSwap(false, true) {
				var in struct {
					Device string `json:"device_id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&in)
				emitted := make(chan struct{}, 1)
				p.h.perms = governance.NewPermissionManager(func(event any) {
					p.h.Emit(event)
					if _, ok := event.(protocol.PermissionRequest); ok {
						select {
						case emitted <- struct{}{}:
						default:
						}
					}
				}, "enforce")
				go func() {
					p.permission.Store(p.h.perms.Request(p.device.Load().(string), "fixture.no_effect", "QA 工具核准", "只驗證原來源回覆，不執行操作", "", "low", "same"))
				}()
				select {
				case <-emitted:
				case <-time.After(time.Second):
					http.Error(w, "fixture permission did not reach emitter", 500)
					return
				}
				blocking := true
				p.exec.mu.Lock()
				p.exec.question = backend.UserInputPayload{RequestID: "qa-question", SessionID: "same", Source: "fixture", Kind: "question", Header: "QA 待回答", Status: "pending", CreatedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), IsBlocking: &blocking, Questions: []protocol.UserInputQuestion{{QuestionID: "qa-option", Text: "QA 問題內容", Type: "single_select", Options: []protocol.UserInputOption{{ID: "yes", Label: "QA 選項", Recommended: true}}}}}
				q := p.exec.question
				p.exec.mu.Unlock()
				p.h.Emit(backend.NewUserInputRequest(q))
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		})
		mux.HandleFunc("/fixture/new-event", func(w http.ResponseWriter, r *http.Request) {
			event, dedup, err := h.events.Insert(ctx, eventinbox.Input{Source: "github", EventKey: "qa-future-rule-event", Kind: "issue.updated", Title: "QA 規則後續事件", Body: "測試啟用後的新事件", Data: json.RawMessage(`{"subject_id":44}`)})
			if err != nil {
				http.Error(w, "fixture insert failed", 500)
				return
			}
			_ = h.publishExternalEvent(event, dedup)
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		})
		mux.HandleFunc("/fixture/stats", func(w http.ResponseWriter, r *http.Request) {
			assignments, _ := h.automation.EventAssignments(ctx)
			snapshot, _ := h.events.SnapshotIncludingArchived(ctx, p.device.Load().(string), 0)
			_ = json.NewEncoder(w).Encode(map[string]any{"session_count": len(h.registry.List()), "target_allowed": h.eventSessionAllowed(sessionObject), "target_state": int(sessionObject.State()), "provider_mock": true, "provider_calls": p.exec.calls.Load(), "answers": p.exec.answered.Load(), "permission_approved": p.permission.Load(), "assignments": assignments, "events": snapshot.Items})
		})
		mux.HandleFunc("/fixture/stop", func(w http.ResponseWriter, r *http.Request) { stopOnce.Do(func() { close(stop) }); w.WriteHeader(204) })
		mux.HandleFunc("/api/automation/v1/", h.ServeAutomationAPI)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if p.device.Load() == nil {
				http.Error(w, "fixture enrollment required before native bootstrap", 503)
				return
			}
			h.ServeWS(w, r)
		})
		listener, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "http://localhost")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Bridge-Device-ID")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			if r.Method == http.MethodOptions {
				w.WriteHeader(204)
				return
			}
			mux.ServeHTTP(w, r)
		})}
		p.server = server
		go server.Serve(listener)
	}
	_ = os.WriteFile(output, []byte(`{"state":"READY","ports":[18766,18767],"provider_mock":true,"production_data_used":false}`), 0600)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(8 * time.Minute)
	defer deadline.Stop()
	running := true
	for running {
		select {
		case <-stop:
			running = false
		case <-deadline.C:
			running = false
		case <-ticker.C:
			for _, p := range peers {
				p.h.drainEventAssignments(ctx)
				p.h.drainAutomationQueue(ctx)
			}
		}
	}
	cancel()
	for _, p := range peers {
		p.h.mu.RLock()
		clients := []*Client{}
		for c := range p.h.clients {
			clients = append(clients, c)
		}
		p.h.mu.RUnlock()
		for _, c := range clients {
			c.conn.Close("fixture complete")
			c.shutdown()
		}
		_ = p.server.Close()
		_ = p.h.messageQueue.Close()
		_ = p.h.automation.Close()
		_ = p.h.events.Close()
		if p.h.work != nil {
			_ = p.h.work.Close()
		}
	}
}
