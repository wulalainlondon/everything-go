package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"os"
	"strings"
	"testing"
)

type recoveryFixtureRPC struct{ scope agentactivity.NativeScope }

func (r recoveryFixtureRPC) Scope() agentactivity.NativeScope { return r.scope }
func (r recoveryFixtureRPC) ReadThread(_ context.Context, p agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	release, e := p.AcquireFor(id, "")
	if e != nil {
		return nil, e
	}
	release()
	parent := "null"
	if id != "" && id != r.scope.RootThread {
		parent = fmt.Sprintf("%q", r.scope.RootThread)
	}
	return json.RawMessage(fmt.Sprintf(`{"thread":{"id":%q,"parentThreadId":%s,"sessionId":"same-tree","turns":[]}}`, id, parent)), nil
}
func (r recoveryFixtureRPC) ListTurns(_ context.Context, p agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	release, e := p.AcquireFor(id, "")
	if e != nil {
		return nil, e
	}
	release()
	return json.RawMessage(`{"data":[{"id":"real-child-turn","status":"completed","rootTurnId":"turn","items":[],"itemsView":"notLoaded"}],"nextCursor":null}`), nil
}
func recoveryEvent() string {
	return fmt.Sprintf(`{"type":"event_msg","payload":{"type":"item_completed","thread_id":%q,"turn_id":"turn","completed_at_ms":1000,"item":{"type":"SubAgentActivity","id":"creation","agent_thread_id":"child","agent_path":"/root/worker","kind":"started","model":"creation-model","reasoning_effort":null}}}`+"\n", exactFixtureThread)
}
func TestHostedRecoveryOriginalIndexLargeHeaderAndPartialTail(t *testing.T) {
	c, path := exactFixture(t)
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	c.rpc.setWriter(&rpcCaptureWriter{writes: make(chan []byte, 1)})
	s := session.NewRegistry().Create("session", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID(exactFixtureThread)
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}
	scope := rpc.Scope()
	source := agentactivity.NewNativeSource(scope)
	rec := &hostedRecovery{scope: scope}
	// Existing final reader's header limit is unchanged; this dedicated metadata
	// projection has its qualified one-MiB header boundary.
	header := fmt.Sprintf(`{"type":"session_meta","payload":{"id":%q,"instructions":%q}}`+"\n", exactFixtureThread, strings.Repeat("PRIVATE", 50000))
	event := recoveryEvent()
	if e := os.WriteFile(path, []byte(header+event[:len(event)-4]), 0600); e != nil {
		t.Fatal(e)
	}
	status := c.recoverHostedPrefix(context.Background(), s, rec, source)
	if status != "partial_tail" || rec.stats.PartialTailBytes == 0 {
		t.Fatal(status, rec.stats)
	}
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteString(event[len(event)-4:])
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	status = c.recoverHostedPrefix(context.Background(), s, rec, source)
	if status != "captured_prefix_partial_inventory" {
		t.Fatal(status)
	}
	p, e := source.Probe(context.Background(), recoveryFixtureRPC{scope})
	if e != nil || len(p.Actors) != 1 || len(p.Actors[0].Rounds) != 1 {
		t.Fatal(p, e)
	}
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), "PRIVATE") || p.InventoryComplete {
		t.Fatal("private header or complete inventory exported")
	}
	// Re-reading has stable native identity and does not append another event.
	c.recoverHostedPrefix(context.Background(), s, rec, source)
	again, _ := source.Probe(context.Background(), recoveryFixtureRPC{scope})
	if again.SourceRevision != p.SourceRevision {
		t.Fatal("replay duplicated activity")
	}
	if e = os.WriteFile(path, []byte(header+strings.Replace(event, `"kind":"started"`, `"kind":"interrupted"`, 1)), 0600); e != nil {
		t.Fatal(e)
	}
	if got := c.recoverHostedPrefix(context.Background(), s, rec, source); got != "source_changed" {
		t.Fatal("changed prefix accepted", got)
	}
}
func TestHostedRecoveryNoIndexNeverDiscoversOtherFiles(t *testing.T) {
	c, path := exactFixture(t)
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	c.rpc.setWriter(&rpcCaptureWriter{writes: make(chan []byte, 1)})
	s := session.NewRegistry().Create("session", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID(exactFixtureThread)
	scope := (hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}).Scope()
	os.WriteFile(path, []byte(fixtureHeader()+recoveryEvent()), 0600)
	c.rolloutByID = nil
	rec := &hostedRecovery{scope: scope}
	if got := c.recoverHostedPrefix(context.Background(), s, rec, agentactivity.NewNativeSource(scope)); got != "indexed_source_unavailable" || rec.pos != 0 || len(c.rolloutByID) != 0 {
		t.Fatal("discovered native source", got)
	}
}

func TestHostedRecoveryPartialPrefixRewriteRetiresOriginalProof(t *testing.T) {
	c, path := exactFixture(t)
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	c.rpc.setWriter(&rpcCaptureWriter{writes: make(chan []byte, 1)})
	s := session.NewRegistry().Create("session", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID(exactFixtureThread)
	scope := (hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}).Scope()
	src := agentactivity.NewNativeSource(scope)
	rec := &hostedRecovery{scope: scope}
	pending := `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}`
	if e := os.WriteFile(path, []byte(fixtureHeader()+recoveryEvent()+pending[:len(pending)-2]), 0600); e != nil {
		t.Fatal(e)
	}
	if got := c.recoverHostedPrefix(context.Background(), s, rec, src); got != "partial_tail" {
		t.Fatal(got)
	}
	if e := os.WriteFile(path, []byte(fixtureHeader()+recoveryEvent()+strings.Replace(pending, `"turn_id":"turn"`, `"turn_id":"evil"`, 1)+"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if got := c.recoverHostedPrefix(context.Background(), s, rec, src); got != "source_changed" {
		t.Fatal(got)
	}
	if src.Usable(scope) {
		t.Fatal("old source proof survived changed partial prefix")
	}
}
