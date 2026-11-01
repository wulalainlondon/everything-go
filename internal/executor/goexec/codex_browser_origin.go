package goexec

import (
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/browsercontrol"
	"everything-go/internal/session"
	"path/filepath"
	"sync"
)

type browserCommandProof struct {
	caller      backend.SessionControlCaller
	fingerprint string
}
type browserOriginState struct {
	entry   string
	mu      sync.Mutex
	proofs  map[string]browserCommandProof
	blocked bool
}

// SetBrowserControlEntry binds native proof to the operator-selected fixed entry.
// Startup calls this before registering daemon notifications.
func (c *Codex) SetBrowserControlEntry(entry string) error {
	if !filepath.IsAbs(entry) || filepath.Clean(entry) != entry {
		return errors.New("browser_control_entry_invalid")
	}
	c.browserControlOrigins.mu.Lock()
	defer c.browserControlOrigins.mu.Unlock()
	if len(c.browserControlOrigins.proofs) != 0 {
		return errors.New("browser_control_entry_in_use")
	}
	c.browserControlOrigins.entry = entry
	return nil
}

func (c *Codex) browserControlEntry() string {
	c.browserControlOrigins.mu.Lock()
	entry := c.browserControlOrigins.entry
	c.browserControlOrigins.mu.Unlock()
	if entry == "" {
		_, entry, _ = browsercontrol.Paths("")
	}
	return entry
}

func (c *Codex) observeBrowserControlNative(s *session.Session, method string, raw json.RawMessage) {
	if s == nil || method != "item/started" || len(raw) > 256<<10 {
		return
	}
	var p struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Item     struct {
			ID, Type, Source string
			CommandActions   []struct{ Type, Command string }
		} `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil || p.ThreadID != s.ResumeID() || p.Item.ID == "" || p.Item.Type != "commandExecution" || (p.Item.Source != "agent" && p.Item.Source != "unifiedExecStartup") || len(p.Item.CommandActions) != 1 || p.Item.CommandActions[0].Type != "unknown" {
		return
	}
	argv, ok := simpleMediaCommand(p.Item.CommandActions[0].Command)
	if !ok || len(argv) < 2 || argv[0] != c.browserControlEntry() || !unambiguousPublicJSON(raw) {
		return
	}
	q, e := browsercontrol.ParseInvocation(argv[1:])
	if e != nil {
		return
	}
	caller, e := c.sessionControlCaller(s, p.TurnID, p.Item.ID)
	if e != nil {
		return
	}
	key := p.ThreadID + "|" + q.ID
	proof := browserCommandProof{caller: caller, fingerprint: q.Fingerprint()}
	state := &c.browserControlOrigins
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.blocked {
		return
	}
	if state.proofs == nil {
		state.proofs = map[string]browserCommandProof{}
	}
	if old, ok := state.proofs[key]; ok {
		if old.caller.TurnID != caller.TurnID || old.caller.ToolCallID != caller.ToolCallID || old.fingerprint != proof.fingerprint {
			state.proofs[key] = browserCommandProof{}
		}
		return
	}
	if len(state.proofs) >= 4096 {
		for k, old := range state.proofs {
			if old.caller.Parent == nil {
				delete(state.proofs, k)
				continue
			}
			if _, e := c.sessionControlCaller(old.caller.Parent, old.caller.TurnID, old.caller.ToolCallID); e != nil {
				delete(state.proofs, k)
			}
		}
		if len(state.proofs) >= 4096 {
			return
		}
	}
	state.proofs[key] = proof
}
func (c *Codex) ResolveBrowserControlCaller(thread, operation, fingerprint string) (backend.SessionControlCaller, error) {
	state := &c.browserControlOrigins
	state.mu.Lock()
	proof, ok := state.proofs[thread+"|"+operation]
	blocked := state.blocked
	state.mu.Unlock()
	if blocked || !ok || proof.caller.Parent == nil || fingerprint != proof.fingerprint {
		return backend.SessionControlCaller{}, errors.New("browser_control_origin_unavailable")
	}
	caller, e := c.sessionControlCaller(proof.caller.Parent, proof.caller.TurnID, proof.caller.ToolCallID)
	if e != nil || caller.RequestID != proof.caller.RequestID {
		return backend.SessionControlCaller{}, errors.New("browser_control_origin_retired")
	}
	c.mediaOrigins.mu.Lock()
	origin, voiceTurn := c.mediaOrigins.turns[thread+"|"+caller.TurnID]
	_, pendingVoice := c.mediaOrigins.pending[thread+"|"+caller.TurnID]
	c.mediaOrigins.mu.Unlock()
	if pendingVoice {
		return backend.SessionControlCaller{}, errors.New("browser_voice_origin_pending")
	}
	if voiceTurn {
		if origin.VoiceID == "" || origin.VoiceID != caller.VoiceID {
			return backend.SessionControlCaller{}, errors.New("browser_voice_retired")
		}
	} else {
		caller.VoiceID = ""
	}
	return caller, nil
}
