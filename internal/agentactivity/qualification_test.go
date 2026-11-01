package agentactivity

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func record(typ string, fields map[string]any) []byte {
	fields["type"] = typ
	b, _ := json.Marshal(map[string]any{"type": "response_item", "metadata": map[string]any{"client_authored": false}, "payload": fields})
	return b
}
func call(id, name string) []byte {
	return record("function_call", map[string]any{"namespace": "collaboration", "name": name, "call_id": id, "arguments": "{}"})
}
func output(id, value string) []byte {
	return record("function_call_output", map[string]any{"call_id": id, "output": value})
}
func feed(t *testing.T, p *HostedQualifier, rows ...[]byte) {
	t.Helper()
	for _, row := range rows {
		if e := p.Observe(row); e != nil {
			t.Fatal(e)
		}
	}
}

func TestHostedReceiptsDoNotInventExecutionRounds(t *testing.T) {
	var p HostedQualifier
	spawn := call("spawn1", "spawn_agent")
	feed(t, &p, spawn, output("spawn1", `{"task_name":"/root/reviewer"}`), call("f1", "followup_task"), output("f1", `{"task_name":"/root/reviewer"}`), call("f2", "followup_task"), output("f2", `{"task_name":"/root/reviewer"}`))
	// Duplicate source receipts never create a new execution or directive.
	feed(t, &p, spawn, output("spawn1", `{"task_name":"/root/reviewer"}`))
	q := p.Snapshot()
	if q.SpawnCalls != 1 || q.SpawnReceipts != 1 || q.FollowupCalls != 2 {
		t.Fatalf("wrong receipt inventory: %+v", q)
	}
	if q.FullActivityReady() || q.ChildRoundSource {
		t.Fatal("orchestration receipts became child execution rounds")
	}
}

func TestCompletedInventoryIsNeitherReviewNorEffectiveModel(t *testing.T) {
	var p HostedQualifier
	feed(t, &p, call("list1", "list_agents"), output("list1", `{"agents":[{"agent_name":"/root/qa","agent_status":{"completed":"PASS! secret model=imaginary"}},{"agent_name":"/root/worker","agent_status":"running"}]}`))
	q := p.Snapshot()
	if q.AgentInventories != 1 || q.CompletedObservations != 1 || q.RunningObservations != 1 {
		t.Fatalf("missing metadata: %+v", q)
	}
	if q.CanonicalReviewSource || q.EffectiveModelSource || q.CompleteActorInventory || q.FullActivityReady() {
		t.Fatal("uncorrelated status was promoted to stronger proof")
	}
	b, _ := json.Marshal(q)
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "imaginary") || strings.Contains(string(b), "/root/qa") {
		t.Fatal("source content leaked")
	}
}

func TestRequestedDimensionsAreNotEffective(t *testing.T) {
	var p HostedQualifier
	feed(t, &p, record("function_call", map[string]any{"namespace": "collaboration", "name": "spawn_agent", "call_id": "s", "arguments": `{"model":"example-requested","reasoning_effort":"high","message":"private"}`}))
	q := p.Snapshot()
	if q.RequestedModels != 1 || q.RequestedEfforts != 1 || q.EffectiveModelSource || q.EffectiveEffortSource {
		t.Fatalf("wrong confidence: %+v", q)
	}
}

func TestTimeoutExitsOnlyTheMatchingWaitCall(t *testing.T) {
	var p HostedQualifier
	feed(t, &p, call("wait1", "wait_agent"), output("foreign", `{"timed_out":true}`), output("wait1", `{"timed_out":true}`), output("wait1", `{"timed_out":true}`))
	q := p.Snapshot()
	if q.WaitCalls != 1 || q.WaitExits != 1 || q.TimeoutExits != 1 || q.CompletedObservations != 0 {
		t.Fatalf("timeout incorrectly completed work: %+v", q)
	}
}

func TestMessagesAreMetadataOnly(t *testing.T) {
	var p HostedQualifier
	feed(t, &p, record("agent_message", map[string]any{"author": "/root/reviewer", "recipient": "/root", "content": []any{map[string]any{"type": "input_text", "text": "Message Type: FINAL_ANSWER\nPASS"}, map[string]any{"type": "encrypted_content", "encrypted_content": "secret-ciphertext"}}, "internal_chat_message_metadata_passthrough": map[string]any{"turn_id": "PARENT_NOT_CHILD"}}))
	q := p.Snapshot()
	b, _ := json.Marshal(q)
	if q.AgentMessages != 1 || q.EncryptedBlocksOmitted != 1 || q.ChildRoundSource || q.CanonicalReviewSource {
		t.Fatalf("message improperly promoted: %+v", q)
	}
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "PARENT_NOT_CHILD") {
		t.Fatal("private content or parent turn leaked")
	}
}

func TestForeignAndClientAuthoredShapesDoNotQualify(t *testing.T) {
	for _, row := range []string{
		`{"type":"response_item","metadata":{"client_authored":true},"payload":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"x"}}`,
		`{"type":"response_item","payload":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"x"}}`,
		`{"type":"response_item","metadata":{"client_authored":false},"payload":{"type":"function_call","namespace":"unrelated","name":"spawn_agent","call_id":"x"}}`,
		`{"type":"response_item","metadata":{"client_authored":false},"payload":{"type":"message","role":"user","text":"spawn_agent"}}`,
	} {
		var p HostedQualifier
		feed(t, &p, []byte(row))
		q := p.Snapshot()
		if q.SpawnCalls != 0 || q.FullActivityReady() {
			t.Fatalf("foreign record qualified: %+v", q)
		}
	}
}

func TestAmbiguousAndOversizedInputFailsClosed(t *testing.T) {
	for _, row := range []string{
		`{"type":"response_item","Type":"response_item"}`,
		`{"payload":{"type":"function_call","ty\u0070e":"function_call"}}`,
		`{"x":1,"X":2}`,
		`{"\u212A":1,"k":2}`,
		`{"x":[]}{"x":1}`,
		strings.Repeat("[", 34) + strings.Repeat("]", 34),
	} {
		var p HostedQualifier
		if p.Observe([]byte(row)) == nil {
			t.Fatalf("ambiguous record accepted: %s", row)
		}
		if p.Snapshot().FullActivityReady() {
			t.Fatal("invalid source became ready")
		}
	}
	var p HostedQualifier
	if p.Observe([]byte(strings.Repeat("x", MaxQualificationRecord+1))) == nil || p.Snapshot().OversizedRecords != 1 {
		t.Fatal("record bound ignored")
	}
}

func TestNestedAmbiguousToolOutputDoesNotCountReceipt(t *testing.T) {
	var p HostedQualifier
	feed(t, &p, call("s", "spawn_agent"))
	if p.Observe(output("s", `{"task_name":"/root/a","TASK_NAME":"/root/forged"}`)) == nil {
		t.Fatal("nested rejection invisible")
	}
	if p.Snapshot().SpawnReceipts != 0 || p.Snapshot().NestedRejections != 1 {
		t.Fatal("ambiguous nested receipt accepted")
	}
}

func TestMissingNullAndWrongInventoryShapesAreNotPositive(t *testing.T) {
	for _, shape := range []string{`{}`, `{"agents":null}`, `{"agents":123}`, `{"agents":[{"agent_name":"/root/a","agent_status":{"completed":null}}]}`, `{"agents":[{"agent_name":"/root/a","agent_status":{"completed":123}}]}`, `{"agents":[{"agent_name":"/root/a","agent_status":{"completed":true}}]}`} {
		var p HostedQualifier
		feed(t, &p, call("l", "list_agents"))
		if p.Observe(output("l", shape)) == nil || p.Snapshot().ShapeRejections != 1 || p.Snapshot().AgentInventories != 0 || p.Snapshot().CompletedObservations != 0 {
			t.Fatalf("invalid inventory counted: %s %+v", shape, p.Snapshot())
		}
	}
	var empty HostedQualifier
	feed(t, &empty, call("l", "list_agents"), output("l", `{"agents":[]}`))
	if empty.Snapshot().AgentInventories != 1 || empty.Snapshot().CompleteActorInventory {
		t.Fatal("empty shape must stay distinct from complete inventory")
	}
}

func TestConflictingCallIDsAreQuarantined(t *testing.T) {
	for _, conflict := range [][]byte{call("x", "wait_agent"), record("function_call", map[string]any{"namespace": "other", "name": "spawn_agent", "call_id": "x", "arguments": "{}"}), record("function_call", map[string]any{"namespace": "collaboration", "name": "spawn_agent", "call_id": "x", "arguments": `{"message":"different"}`})} {
		var p HostedQualifier
		feed(t, &p, call("x", "spawn_agent"))
		if p.Observe(conflict) == nil || p.Snapshot().CallConflicts != 1 {
			t.Fatal("conflicting reuse treated as duplicate")
		}
		if p.Observe(output("x", `{"task_name":"/root/a","timed_out":true}`)) == nil || p.Snapshot().SpawnReceipts != 0 || p.Snapshot().WaitExits != 0 || p.Snapshot().QuarantinedOutputs != 1 {
			t.Fatal("quarantined correlation produced positive output")
		}
	}
}

func TestCallCapacityOmissionsAreVisible(t *testing.T) {
	var p HostedQualifier
	for i := 0; i < 4096; i++ {
		feed(t, &p, call(fmt.Sprint(i), "spawn_agent"), output(fmt.Sprint(i), `{"task_name":"/root/a"}`))
	}
	if p.Observe(call("over", "spawn_agent")) == nil || p.Snapshot().CapacityRejections != 1 {
		t.Fatal("capacity omission hidden")
	}
	feed(t, &p, output("over", `{"task_name":"/root/a"}`))
	if p.Snapshot().SpawnReceipts != 4096 || p.Snapshot().UnmatchedOutputs != 1 {
		t.Fatal("over-bound call was falsely correlated")
	}
}

func TestNestedArgumentsAndWaitShapeRejectionsAreVisible(t *testing.T) {
	var p HostedQualifier
	bad := record("function_call", map[string]any{"namespace": "collaboration", "name": "spawn_agent", "call_id": "s", "arguments": `{"model":"a","MODEL":"b"}`})
	if p.Observe(bad) == nil || p.Snapshot().NestedRejections != 1 || p.Snapshot().SpawnCalls != 0 {
		t.Fatal("nested args rejection hidden")
	}
	if p.Observe(output("s", `{"task_name":"/root/a"}`)) == nil || p.Snapshot().QuarantinedOutputs != 1 {
		t.Fatal("invalid call produced a receipt")
	}
	feed(t, &p, call("w", "wait_agent"))
	if p.Observe(output("w", `{}`)) == nil || p.Snapshot().ShapeRejections != 1 || p.Snapshot().WaitExits != 0 {
		t.Fatal("missing timeout shape counted as qualified exit")
	}
}

func TestBoundedStreamRetainsPartialTailAndRejectionCounts(t *testing.T) {
	for _, n := range []int{12, MaxQualificationRecord + 32, (MaxQualificationRecord + 1) * 2} {
		var p HostedQualifier
		if e := p.ReadRecords(strings.NewReader(strings.Repeat("x", n))); e != nil {
			t.Fatal(e)
		}
		q := p.Snapshot()
		if q.Records != 1 || q.PartialTailRecords != 1 || q.PartialTailBytes != int64(n) {
			t.Fatalf("tail omitted/double counted: %d %+v", n, q)
		}
		if (q.OversizedRecords > 0) != (n > MaxQualificationRecord) {
			t.Fatal("oversized tail classification wrong")
		}
	}
	var p HostedQualifier
	stream := string(call("s", "spawn_agent")) + "\n" + string(output("s", `{"task_name":"/root/a","TASK_NAME":"/root/b"}`)) + "\n" + strings.Repeat("x", MaxQualificationRecord+20) + "\n" + "tail"
	if e := p.ReadRecords(strings.NewReader(stream)); e != nil {
		t.Fatal(e)
	}
	q := p.Snapshot()
	if q.NestedRejections != 1 || q.OversizedRecords != 1 || q.PartialTailRecords != 1 || q.PartialTailBytes != 4 || q.SpawnReceipts != 0 {
		t.Fatalf("stream rejection summary incorrect: %+v", q)
	}
}
