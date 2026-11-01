package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/browsercontrol"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"testing"
)

func browserControlOriginFixture(t *testing.T) (*Codex, *session.Session) {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("line-original", "LINE", t.TempDir(), backend.Codex, "", "read-only", "original-thread")
	st := c.state(s.ID)
	st.threadID = s.ResumeID()
	st.turnActive = true
	st.currentTurnID = "original-turn"
	st.reqID = "original-request"
	return c, s
}
func browserControlCommandEvent(t *testing.T, c *Codex, s *session.Session, command, source, tool, thread string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"threadId": thread, "turnId": "original-turn", "item": map[string]any{"id": tool, "type": "commandExecution", "source": source, "commandActions": []map[string]string{{"type": "unknown", "command": command}}}})
	c.observeBrowserControlNative(s, "item/started", b)
}
func TestBrowserControlCLIOriginRequiresExactNativeCommandPayloadAndCurrentCaller(t *testing.T) {
	c, s := browserControlOriginFixture(t)
	home, _ := os.UserHomeDir()
	command := filepath.Join(home, ".local/bin/bridge-browser") + " --id bop_original_readonly --method status --params '{}'"
	q, e := browsercontrol.ParseInvocation([]string{"--id", "bop_original_readonly", "--method", "status", "--params", "{}"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("environment without native proof accepted")
	}
	browserControlCommandEvent(t, c, s, command, "unifiedExecStartup", "native-exec-original", s.ResumeID())
	caller, e := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint())
	if e != nil || caller.RequestID != "original-request" || caller.ToolCallID != "native-exec-original" {
		t.Fatal(caller, e)
	}
	if _, e = c.ResolveBrowserControlCaller("other-thread", q.ID, q.Fingerprint()); e == nil {
		t.Fatal("foreign thread accepted")
	}
	if _, e = c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, "changed"); e == nil {
		t.Fatal("payload substitution accepted")
	}
	c.state(s.ID).currentTurnID = "new-turn"
	if _, e = c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("retired caller accepted")
	}
}
func TestBrowserControlCLIRejectsQuotedMentionsShellChainsAndDelegatedCaller(t *testing.T) {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".local/bin/bridge-browser") + " --id bop_rejected_intent --method status"
	for _, command := range []string{"echo '" + base + "'", base + "; true", "/tmp/bridge-browser --id bop_rejected_intent --method status", base + " | cat"} {
		c, s := browserControlOriginFixture(t)
		browserControlCommandEvent(t, c, s, command, "agent", "actual-call", s.ResumeID())
		q, _ := browsercontrol.ParseInvocation([]string{"--id", "bop_rejected_intent", "--method", "status"})
		if _, e := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
			t.Fatal("nonliteral invocation accepted")
		}
	}
	c, s := browserControlOriginFixture(t)
	s.ID = "s_dg_child"
	browserControlCommandEvent(t, c, s, base, "agent", "actual-call", s.ResumeID())
	q, _ := browsercontrol.ParseInvocation([]string{"--id", "bop_rejected_intent", "--method", "status"})
	if _, e := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("delegation acquired voice transfer")
	}
}

func TestBrowserControlDedicatedEntryOnlyAcceptsItsOwnNativeCommand(t *testing.T) {
	c, s := browserControlOriginFixture(t)
	entry := filepath.Join(t.TempDir(), "bridge-browser")
	if err := c.SetBrowserControlEntry(entry); err != nil {
		t.Fatal(err)
	}
	q, _ := browsercontrol.ParseInvocation([]string{"--id", "bop_dedicated_read", "--method", "status"})
	_, defaultEntry, _ := browsercontrol.Paths("")
	for _, wrong := range []string{defaultEntry, filepath.Join(t.TempDir(), "bridge-browser")} {
		browserControlCommandEvent(t, c, s, wrong+" --id bop_dedicated_read --method status", "agent", "foreign-entry", s.ResumeID())
		if _, err := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); err == nil {
			t.Fatal("foreign entry acquired proof")
		}
	}
	browserControlCommandEvent(t, c, s, entry+" --id bop_dedicated_read --method status", "agent", "own-entry", s.ResumeID())
	if _, err := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); err != nil {
		t.Fatal(err)
	}
	if err := c.SetBrowserControlEntry(defaultEntry); err == nil {
		t.Fatal("live proof rebound to another entry")
	}
	if err := c.SetBrowserControlEntry("relative"); err == nil {
		t.Fatal("relative entry accepted")
	}
}

func TestBrowserTextProofDoesNotAcquireVoiceAndSurvivesUnrelatedCallEnd(t *testing.T) {
	c, s := browserControlOriginFixture(t)
	home, _ := os.UserHomeDir()
	q, _ := browsercontrol.ParseInvocation([]string{"--id", "bop_plain_text_request", "--method", "status"})
	browserControlCommandEvent(t, c, s, filepath.Join(home, ".local/bin/bridge-browser")+" --id bop_plain_text_request --method status", "agent", "native-call", s.ResumeID())
	caller, e := c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint())
	if e != nil || caller.VoiceID != "" || caller.RequestID != "original-request" {
		t.Fatal(caller, e)
	}
	c.state(s.ID).turnActive = false
	if _, e = c.ResolveBrowserControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("finished text request acquired a new effect")
	}
}
