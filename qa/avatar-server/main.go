// Local, synthetic QA only. No live credentials, providers or native execution.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"everything-go/internal/workspace"
	"flag"
	"fmt"
	"github.com/coder/websocket"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func data(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func main() {
	enabled := flag.Bool("local-fixture", false, "required: isolated synthetic fixture")
	flag.Parse()
	if !*enabled {
		log.Fatal("local-fixture flag required")
	}
	dir, err := os.MkdirTemp("", "bridge-avatar-qa-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s, err := workspace.Open(dir, "avatar-qa")
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	owner := s.Owner("fixture-device", strings.Repeat("a", 64))
	n := 0
	apply := func(actor workspace.Actor, c workspace.Command) workspace.Result {
		n++
		c.Authority = "avatar-qa"
		c.MutationID = fmt.Sprintf("fixture-init-%08d", n)
		r, e := s.Apply(ctx, actor, c)
		if e != nil {
			log.Fatal(e)
		}
		return r
	}
	w := apply(owner, workspace.Command{Action: "create_workspace", Data: data(map[string]any{"name": "Avatar QA"})}).Workspace
	room := apply(owner, workspace.Command{Action: "create_room", WorkspaceID: w.ID, Data: data(map[string]any{"name": "頭像驗收", "description": "本機隔離樣本，沒有外部帳號或模型"})}).Room
	acl := room.ACLRevision
	invite := apply(owner, workspace.Command{Action: "create_invite", RoomID: room.ID, ExpectedACL: &acl, Data: data(map[string]any{"role": "viewer", "expires_at": time.Now().Add(time.Hour).UnixMilli()})})
	viewer, _, err := s.AcceptInvite(ctx, invite.InviteCode, strings.Repeat("q", 48), "fixture-viewer-device", "同名成員")
	if err != nil {
		log.Fatal(err)
	}
	snap := apply(owner, workspace.Command{Action: "snapshot", RoomID: room.ID}).Snapshot
	acl = snap.Room.ACLRevision
	agent := workspace.Agent{Name: "研究搭檔", Description: "Local fixture only", Ready: true, Backend: "codex", Model: "fixture", SessionID: "qa-avatar-no-native-session", ThreadID: "qa-avatar-no-native-thread", Config: 1, RoleRevision: 1, RoleHash: fmt.Sprintf("%x", sha256.Sum256([]byte("Local fixture only"))), RoomExclusive: true}
	ae := apply(owner, workspace.Command{Action: "bind_agent", RoomID: room.ID, ExpectedACL: &acl, Data: data(map[string]any{"session_id": agent.SessionID}), VerifiedAgent: &agent}).Entity
	apply(owner, workspace.Command{Action: "post_message", RoomID: room.ID, ExpectedACL: &acl, Data: data(map[string]any{"body": "A 風格頭像，跟著原身分一起顯示。"})})
	// Synthetic finalized agent post, separate from real provider execution.
	apply(owner, workspace.Command{Action: "post_message", RoomID: room.ID, ExpectedACL: &acl, Data: data(map[string]any{"body": "可在成員與搭檔設定更換頭像。"})})
	var mu sync.Mutex
	clients := map[*websocket.Conn]bool{}
	http.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://127.0.0.1:48621")
		json.NewEncoder(w).Encode(map[string]any{"room": room.ID, "agent": ae.ID, "owner": owner.ID, "viewer": viewer.ID, "fixture": true})
	})
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		actor := owner
		if r.URL.Query().Get("persona") == "viewer" {
			actor = viewer
		}
		conn, e := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"127.0.0.1:48621"}})
		if e != nil {
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(1024 * 1024)
		mu.Lock()
		clients[conn] = true
		mu.Unlock()
		defer func() { mu.Lock(); delete(clients, conn); mu.Unlock() }()
		for {
			_, body, e := conn.Read(r.Context())
			if e != nil {
				return
			}
			var in struct {
				RequestID string            `json:"request_id"`
				Command   workspace.Command `json:"workspace"`
			}
			if json.Unmarshal(body, &in) != nil {
				return
			}
			result, err := s.Apply(r.Context(), actor, in.Command)
			out := map[string]any{"type": "workspace_result", "request_id": in.RequestID, "authority": "avatar-qa", "principal_id": actor.ID, "ok": err == nil, "mutation_state": "committed", "data": result}
			if err != nil {
				out["error"] = err.Error()
				out["mutation_state"] = "refused"
			}
			b, _ := json.Marshal(out)
			if conn.Write(r.Context(), websocket.MessageText, b) != nil {
				return
			}
			if err == nil && !workspace.ReadAction(in.Command.Action) {
				mu.Lock()
				for c := range clients {
					_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"workspace_changed","authority":"avatar-qa","ok":true}`))
				}
				mu.Unlock()
			}
		}
	})
	log.Print("LOCAL_SYNTHETIC_AVATAR_QA http://127.0.0.1:48620")
	log.Fatal(http.ListenAndServe("127.0.0.1:48620", nil))
}
