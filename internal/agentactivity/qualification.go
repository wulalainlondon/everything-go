// Package agentactivity qualifies observable provider data before UI adoption.
// Qualification is diagnostic only: it supplies no authority, task relation,
// execution round, QA verdict, or permission to read another source.
package agentactivity

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxQualificationRecord = 256 << 10

// HostedQualification deliberately distinguishes orchestration metadata from
// child execution evidence. A parent rollout turn_id is never a child turn_id.
type HostedQualification struct {
	Records                int   `json:"records"`
	OversizedRecords       int   `json:"oversized_records"`
	MalformedRecords       int   `json:"outer_malformed_records"`
	NestedRejections       int   `json:"nested_rejections"`
	ShapeRejections        int   `json:"shape_rejections"`
	CapacityRejections     int   `json:"capacity_rejections"`
	CallConflicts          int   `json:"call_conflicts"`
	DuplicateCalls         int   `json:"duplicate_calls"`
	DuplicateOutputs       int   `json:"duplicate_outputs"`
	UnmatchedOutputs       int   `json:"unmatched_outputs"`
	QuarantinedOutputs     int   `json:"quarantined_outputs"`
	UnknownActorStatuses   int   `json:"unknown_actor_statuses"`
	PartialTailRecords     int   `json:"partial_tail_records"`
	PartialTailBytes       int64 `json:"partial_tail_bytes"`
	SpawnCalls             int   `json:"spawn_calls"`
	FollowupCalls          int   `json:"followup_calls"`
	SpawnReceipts          int   `json:"spawn_receipts"`
	AgentInventories       int   `json:"agent_inventories"`
	RunningObservations    int   `json:"running_observations"`
	CompletedObservations  int   `json:"completed_observations"`
	WaitCalls              int   `json:"wait_calls"`
	WaitExits              int   `json:"wait_exits"`
	TimeoutExits           int   `json:"timeout_exits"`
	AgentMessages          int   `json:"agent_messages"`
	EncryptedBlocksOmitted int   `json:"encrypted_blocks_omitted"`
	RequestedModels        int   `json:"requested_models"`
	RequestedEfforts       int   `json:"requested_efforts"`
	ChildRoundSource       bool  `json:"child_execution_round_source"`
	EffectiveModelSource   bool  `json:"effective_model_source"`
	EffectiveEffortSource  bool  `json:"effective_effort_source"`
	ChildToolSource        bool  `json:"child_tool_source"`
	CanonicalReviewSource  bool  `json:"canonical_review_source"`
	CompleteActorInventory bool  `json:"complete_actor_inventory"`
}

// FullActivityReady is the M0 gate. Syntax, names, and completion text cannot
// supply an execution-round binding. This reader intentionally leaves every
// execution/canonical capability false until a distinct verified owner exists.
func (q HostedQualification) FullActivityReady() bool {
	return q.ChildRoundSource
}

// HostedQualifier counts supported shapes without retaining content, titles,
// tool arguments, model values, actor IDs, private reasoning or encrypted data.
// Its caller owns source selection/header verification and bounded file reads.
// It is not installed on a production socket or a provider callback.
type HostedQualifier struct {
	q       HostedQualification
	pending map[string]*qualifiedCall
}

type qualifiedCall struct {
	kind                        string
	fingerprint                 [32]byte
	consumed, conflicted, valid bool
}

type rejection string

func (r rejection) Error() string { return string(r) }
func isRejection(err error) bool  { var r rejection; return errors.As(err, &r) }

func (p *HostedQualifier) Snapshot() HostedQualification { return p.q }

func (p *HostedQualifier) Observe(line []byte) error {
	p.q.Records++
	if len(line) > MaxQualificationRecord {
		p.q.OversizedRecords++
		return rejection("qualification_record_bound")
	}
	if !uniqueJSON(line) {
		p.q.MalformedRecords++
		return rejection("qualification_ambiguous_or_invalid_json")
	}
	var row struct {
		Type     string `json:"type"`
		Metadata struct {
			ClientAuthored *bool `json:"client_authored"`
		} `json:"metadata"`
		Payload struct {
			Type      string          `json:"type"`
			Name      string          `json:"name"`
			Namespace string          `json:"namespace"`
			CallID    string          `json:"call_id"`
			Arguments json.RawMessage `json:"arguments"`
			Output    json.RawMessage `json:"output"`
			Author    string          `json:"author"`
			Recipient string          `json:"recipient"`
			Content   []struct {
				Type string `json:"type"`
			} `json:"content"`
		} `json:"payload"`
	}
	if json.Unmarshal(line, &row) != nil {
		p.q.MalformedRecords++
		return rejection("qualification_invalid_record")
	}
	if row.Type != "response_item" || row.Metadata.ClientAuthored == nil || *row.Metadata.ClientAuthored {
		return nil
	}
	r := row.Payload
	switch r.Type {
	case "function_call":
		fingerprint := sha256.Sum256([]byte(r.Namespace + "\x00" + r.Name + "\x00" + string(r.Arguments)))
		if previous := p.pending[r.CallID]; previous != nil {
			if previous.fingerprint != fingerprint {
				if !previous.conflicted {
					p.q.CallConflicts++
				}
				previous.conflicted = true
				return rejection("qualification_call_conflict")
			}
			p.q.DuplicateCalls++
			return nil
		}
		if r.Namespace != "collaboration" || !shortID(r.CallID) {
			return nil
		}
		switch r.Name {
		case "spawn_agent", "followup_task", "list_agents", "wait_agent":
		default:
			return nil
		}
		if p.pending == nil {
			p.pending = make(map[string]*qualifiedCall)
		}
		// No eviction that could turn an unrelated late result into a match.
		if len(p.pending) >= 4096 {
			p.q.CapacityRejections++
			return rejection("qualification_call_bound")
		}
		entry := &qualifiedCall{kind: r.Name, fingerprint: fingerprint}
		p.pending[r.CallID] = entry
		b := jsonObject(r.Arguments)
		if b == nil {
			p.q.NestedRejections++
			return rejection("qualification_nested_arguments")
		}
		var args struct {
			Model  *string `json:"model"`
			Effort *string `json:"reasoning_effort"`
		}
		if json.Unmarshal(b, &args) != nil {
			p.q.ShapeRejections++
			return rejection("qualification_argument_shape")
		}
		entry.valid = true
		switch r.Name {
		case "spawn_agent":
			p.q.SpawnCalls++
		case "followup_task":
			p.q.FollowupCalls++
		case "wait_agent":
			p.q.WaitCalls++
		}
		if r.Name == "spawn_agent" {
			if args.Model != nil && *args.Model != "" {
				p.q.RequestedModels++
			}
			if args.Effort != nil && *args.Effort != "" {
				p.q.RequestedEfforts++
			}
		}
	case "function_call_output":
		entry := p.pending[r.CallID]
		if entry == nil {
			p.q.UnmatchedOutputs++
			return nil
		}
		if entry.conflicted || !entry.valid {
			p.q.QuarantinedOutputs++
			return rejection("qualification_quarantined_output")
		}
		if entry.consumed {
			p.q.DuplicateOutputs++
			return nil
		}
		entry.consumed = true
		b := jsonObject(r.Output)
		if b == nil {
			p.q.NestedRejections++
			return rejection("qualification_nested_output")
		}
		switch entry.kind {
		case "spawn_agent":
			var out struct {
				TaskName string `json:"task_name"`
			}
			if json.Unmarshal(b, &out) != nil || !actorPath(out.TaskName) {
				p.q.ShapeRejections++
				return rejection("qualification_spawn_receipt_shape")
			}
			p.q.SpawnReceipts++
		case "list_agents":
			var out struct {
				Agents *[]struct {
					Name   string          `json:"agent_name"`
					Status json.RawMessage `json:"agent_status"`
				} `json:"agents"`
			}
			if json.Unmarshal(b, &out) != nil || out.Agents == nil || len(*out.Agents) > 256 {
				p.q.ShapeRejections++
				return rejection("qualification_inventory_shape")
			}
			running, completed, unknown := 0, 0, 0
			for _, a := range *out.Agents {
				if a.Name != "/root" && !actorPath(a.Name) {
					p.q.ShapeRejections++
					return rejection("qualification_actor_shape")
				}
				var state *string
				if json.Unmarshal(a.Status, &state) == nil && state != nil {
					if a.Name != "/root" {
						if *state == "running" {
							running++
						} else {
							unknown++
						}
					}
					continue
				}
				var status map[string]json.RawMessage
				if json.Unmarshal(a.Status, &status) != nil || len(status) != 1 {
					p.q.ShapeRejections++
					return rejection("qualification_actor_status_shape")
				}
				for kind, value := range status {
					var message *string
					if json.Unmarshal(value, &message) != nil || message == nil {
						p.q.ShapeRejections++
						return rejection("qualification_actor_status_payload")
					}
					if a.Name != "/root" {
						if kind == "completed" {
							completed++
						} else {
							unknown++
						}
					}
				}
			}
			p.q.AgentInventories++
			p.q.RunningObservations += running
			p.q.CompletedObservations += completed
			p.q.UnknownActorStatuses += unknown
		case "wait_agent":
			var out struct {
				TimedOut *bool `json:"timed_out"`
			}
			if json.Unmarshal(b, &out) != nil || out.TimedOut == nil {
				p.q.ShapeRejections++
				return rejection("qualification_wait_exit_shape")
			}
			p.q.WaitExits++
			if *out.TimedOut {
				p.q.TimeoutExits++
			}
		}
	case "agent_message":
		if !actorPath(r.Author) || r.Recipient != "/root" {
			return nil
		}
		p.q.AgentMessages++
		for _, c := range r.Content {
			if c.Type == "encrypted_content" {
				p.q.EncryptedBlocksOmitted++
			}
		}
	}
	return nil
}

func shortID(s string) bool {
	return len(s) > 0 && len(s) <= 256 && !strings.ContainsAny(s, "\r\n\x00")
}
func actorPath(s string) bool {
	if !strings.HasPrefix(s, "/root/") || len(s) > 256 {
		return false
	}
	for _, part := range strings.Split(strings.TrimPrefix(s, "/root/"), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				return false
			}
		}
	}
	return true
}

func jsonObject(raw json.RawMessage) []byte {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		raw = []byte(s)
	}
	raw = bytes.TrimSpace(raw)
	if !uniqueJSON(raw) || len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	return raw
}

// Reject duplicate keys, including escaped aliases, at every level. The depth
// bound also prevents an attacker-controlled record from exhausting the stack.
func uniqueJSON(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	var walk func(int) bool
	walk = func(depth int) bool {
		if depth > 32 {
			return false
		}
		t, err := d.Token()
		if err != nil {
			return false
		}
		mark, container := t.(json.Delim)
		if !container {
			return true
		}
		switch mark {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				t, e := d.Token()
				if e != nil {
					return false
				}
				k, ok := t.(string)
				if !ok {
					return false
				}
				k = foldKey(k)
				if seen[k] {
					return false
				}
				seen[k] = true
				if !walk(depth + 1) {
					return false
				}
			}
			t, e := d.Token()
			return e == nil && t == json.Delim('}')
		case '[':
			for d.More() {
				if !walk(depth + 1) {
					return false
				}
			}
			t, e := d.Token()
			return e == nil && t == json.Delim(']')
		}
		return false
	}
	if !walk(0) {
		return false
	}
	_, e := d.Token()
	return e != nil && json.Valid(raw)
}

func foldKey(key string) string {
	var out strings.Builder
	for _, c := range key {
		min := c
		for next := unicode.SimpleFold(c); next != c; next = unicode.SimpleFold(next) {
			if next < min {
				min = next
			}
		}
		out.WriteRune(min)
	}
	return out.String()
}
