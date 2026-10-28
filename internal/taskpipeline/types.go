// Package taskpipeline coordinates original task receipts. It never owns a
// provider or an execution queue. Its outbox is admitted by the original writer.
package taskpipeline

type Identity struct {
	Authority string `json:"authority_instance_id"`
	Session   string `json:"session_id"`
	Thread    string `json:"thread_id"`
	Revision  uint64 `json:"config_revision"`
}
type Locator struct {
	Kind      string `json:"kind"`
	Authority string `json:"authority_instance_id"`
	TaskID    string `json:"task_id"`
	NativeID  string `json:"native_id"`
	RoundID   string `json:"round_id"`
	Session   string `json:"session_id"`
	Request   string `json:"request_id"`
	Thread    string `json:"thread_id"`
	Revision  uint64 `json:"config_revision"`
}
type Predicate struct {
	Kind          string `json:"kind"`
	InputRevision uint64 `json:"input_revision"`
	InputHash     string `json:"input_hash"`
	ArtifactHash  string `json:"artifact_hash"`
	DecisionRef   string `json:"decision_ref"`
}
type Action struct {
	Kind            string   `json:"kind"`
	Target          Identity `json:"target"`
	OriginalTask    *Locator `json:"original_task"`
	Instruction     string   `json:"instruction"`
	InstructionHash string   `json:"instruction_hash"`
	Disclosure      string   `json:"disclosure"`
}
type Budget struct {
	DeadlineMS    int64 `json:"deadline_ms"`
	MaxEffects    int   `json:"max_effects"`
	MaxModelTurns int   `json:"max_model_turns"`
	MaxNoProgress int   `json:"max_no_progress"`
}
type Authorization struct {
	SourceRef string `json:"source_ref"`
	Confirmed bool   `json:"confirmed"`
	Budget    Budget `json:"budget"`
}
type WorkItemRef struct {
	ProjectID string `json:"project_id"`
	ItemID    string `json:"item_id"`
	Revision  uint64 `json:"revision"`
}
type Step struct {
	ID        string       `json:"id"`
	Title     string       `json:"title"`
	Required  bool         `json:"required"`
	Source    *Locator     `json:"source"`
	Predicate Predicate    `json:"predicate"`
	Action    *Action      `json:"action"`
	WorkItem  *WorkItemRef `json:"work_item"`
}
type Mission struct {
	RecoveryAction *Action       `json:"recovery_action"`
	Goal           string        `json:"goal"`
	Completion     string        `json:"completion"`
	Steps          []Step        `json:"steps"`
	Authorization  Authorization `json:"authorization"`
}
type Watch struct {
	Source        Locator       `json:"source"`
	Predicate     Predicate     `json:"predicate"`
	Destination   Identity      `json:"destination"`
	Mode          string        `json:"mode"`
	CatchUp       bool          `json:"catch_up"`
	Action        *Action       `json:"action"`
	Authorization Authorization `json:"authorization"`
}
type Edge struct {
	ID        string   `json:"id"`
	From      []string `json:"from"`
	To        string   `json:"to"`
	Condition string   `json:"condition"`
	Join      string   `json:"join"`
	Output    string   `json:"output"`
}
type Pipeline struct {
	Name          string        `json:"name"`
	Nodes         []Step        `json:"nodes"`
	Edges         []Edge        `json:"edges"`
	Authorization Authorization `json:"authorization"`
}
type Checkpoint struct {
	Revision     uint64   `json:"revision"`
	StepID       string   `json:"step_id"`
	Processed    []string `json:"processed_event_ids"`
	ProgressHash string   `json:"progress_hash"`
	Outcome      string   `json:"outcome"`
	Blockers     []string `json:"blockers"`
	RequestID    string   `json:"request_id"`
}
type StepState struct {
	ID             string `json:"id"`
	State          string `json:"state"`
	EvidenceHash   string `json:"evidence_hash"`
	ContinuationID string `json:"continuation_id"`
	Reason         string `json:"reason"`
}
type Record struct {
	AccessMode         string      `json:"access_mode"`
	NoProgressCount    uint64      `json:"no_progress_count"`
	RecoveryGeneration uint64      `json:"recovery_generation"`
	ID                 string      `json:"id"`
	Kind               string      `json:"kind"`
	Revision           uint64      `json:"revision"`
	DefinitionRevision uint64      `json:"definition_revision"`
	State              string      `json:"state"`
	Manager            Identity    `json:"manager"`
	Mission            *Mission    `json:"mission"`
	Watch              *Watch      `json:"watch"`
	Pipeline           *Pipeline   `json:"pipeline"`
	Checkpoint         *Checkpoint `json:"checkpoint"`
	Steps              []StepState `json:"steps"`
	BlockedReason      string      `json:"blocked_reason"`
	CreatedAt          int64       `json:"created_at_ms"`
	UpdatedAt          int64       `json:"updated_at_ms"`
}

// Scope and Issuer have no wire representation. Only the authenticated gateway
// or the server-owned authorization enforcer constructs them.
type Scope struct {
	Principal  string
	Generation uint64
	Manager    Identity
}
type Issuer struct {
	Scope            Scope
	Kind             string
	SourceRequest    string
	SourceGeneration uint64
	GrantRevision    uint64
	Targets          map[string]uint64
}
type Fact struct {
	ConsumedRevision uint64 `json:"consumed_input_revision"`
	ConsumedHash     string `json:"consumed_input_hash"`
	EvidenceStatus   string `json:"evidence_status"`

	Source        Locator `json:"source"`
	Store         string  `json:"store"`
	Sequence      uint64  `json:"sequence"`
	InputRevision uint64  `json:"input_revision"`
	InputHash     string  `json:"input_hash"`
	Admission     string  `json:"admission"`
	NativeStart   string  `json:"native_start"`
	Terminal      string  `json:"terminal"`
	Final         string  `json:"final"`
	SealID        string  `json:"seal_id"`
	FullHash      string  `json:"full_hash"`
	Bytes         int     `json:"total_bytes"`
	QA            string  `json:"qa"`
	QARound       string  `json:"qa_round_id"`
	QAOwnerRound  string  `json:"qa_owner_round_id"`
	ArtifactHash  string  `json:"artifact_hash"`
	DecisionRef   string  `json:"decision_ref"`
	ObservedAt    int64   `json:"observed_at_ms"`
}
type Event struct {
	ID              string `json:"id"`
	Sequence        uint64 `json:"sequence"`
	RecordID        string `json:"record_id"`
	NodeID          string `json:"node_id"`
	State           string `json:"state"`
	Fact            Fact   `json:"fact"`
	AppliedRevision uint64 `json:"applied_revision"`
	Reason          string `json:"reason"`
}
type NativeOperationRef struct {
	IntentID  string `json:"intent_id"`
	Operation string `json:"operation"`
	InputHash string `json:"input_hash"`
}
type Continuation struct {
	NativeOperations   []NativeOperationRef `json:"native_operations"`
	OriginalNodeID     string               `json:"original_node_id"`
	Generation         uint64               `json:"generation"`
	StartProgressHash  string               `json:"start_progress_hash"`
	ID                 string               `json:"id"`
	RecordID           string               `json:"record_id"`
	DefinitionRevision uint64               `json:"definition_revision"`
	NodeID             string               `json:"node_id"`
	State              string               `json:"state"`
	Action             Action               `json:"action"`
	RequestID          string               `json:"request_id"`
	Evidence           []Fact               `json:"evidence"`
	Attempt            int                  `json:"attempt"`
	Fence              uint64               `json:"fence"`
	Reason             string               `json:"reason"`
	OriginalReceipt    *EffectReceipt       `json:"original_receipt"`
	NativeStart        string               `json:"native_start"`
	Terminal           string               `json:"terminal"`
	CheckpointRevision uint64               `json:"checkpoint_revision"`
}
type EffectReceipt struct {
	Kind      string `json:"kind"`
	Authority string `json:"authority_instance_id"`
	Session   string `json:"session_id"`
	Request   string `json:"request_id"`
	NativeID  string `json:"native_record_id"`
	Key       string `json:"operation_key"`
	InputHash string `json:"input_hash"`
}
type NativeProvenance struct {
	Source            Identity `json:"source"`
	RequestID         string   `json:"request_id"`
	NativeTurnID      string   `json:"native_turn_id"`
	ToolCallID        string   `json:"tool_call_id"`
	ProcessGeneration string   `json:"process_generation"`
	CorrelationID     string   `json:"correlation_id"`
	ActorKind         string   `json:"actor_kind"`
	Tool              string   `json:"tool"`
}
type Receipt struct {
	Provenance   *NativeProvenance `json:"provenance"`
	Kind         string            `json:"kind"`
	IntentID     string            `json:"intent_id"`
	InputHash    string            `json:"input_hash"`
	Operation    string            `json:"operation"`
	State        string            `json:"state"`
	Record       *Record           `json:"record"`
	Continuation *Continuation     `json:"continuation"`
	NoEffect     bool              `json:"no_effect"`
	Reason       string            `json:"reason"`
}
