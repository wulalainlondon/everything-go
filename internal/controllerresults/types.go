// Package controllerresults is the durable controller receipt inbox/outbox.
// It shares the original dispatch database; it never owns a provider scheduler.
package controllerresults

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

var ErrScope = errors.New("controller_result_scope_changed")
var ErrConflict = errors.New("controller_result_revision_conflict")
var ErrGap = errors.New("controller_result_cursor_gap")
var ErrBudget = errors.New("controller_result_budget_exhausted")

type Identity struct {
	Authority string `json:"authority_instance_id"`
	Session   string `json:"session_id"`
	Thread    string `json:"thread_id"`
	Revision  uint64 `json:"config_revision"`
}
type Scope struct {
	Parent        Identity `json:"parent"`
	Generation    uint64   `json:"generation"`
	GrantRevision uint64   `json:"-"`
}

func (s Scope) Key() string { return Hash(s) }
func Hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Final struct {
	SealID  string `json:"seal_id"`
	Hash    string `json:"full_hash"`
	Bytes   int    `json:"byte_length"`
	Request string `json:"owner_request_id"`
}
type Execution struct {
	Target  Identity `json:"target"`
	Request string   `json:"request_id"`
	Turn    string   `json:"native_turn_id"`
}
type Version struct {
	ID      string `json:"group_id"`
	Version uint64 `json:"version"`
	Hash    string `json:"fact_hash"`
}
type Group struct {
	MissionApplications []MissionApplication `json:"mission_applications,omitempty"`
	ID                  string               `json:"group_id"`
	Version             uint64               `json:"version"`
	FactHash            string               `json:"fact_hash"`
	Execution           Execution            `json:"execution"`
	State               string               `json:"state"`
	Final               *Final               `json:"final"`
	ConflictHashes      []string             `json:"conflict_hashes"`
	MemberCount         int                  `json:"member_count"`
	Unread              bool                 `json:"unread"`
	Acknowledged        bool                 `json:"acknowledged"`
	HumanSeen           bool                 `json:"human_seen"`
	NativeRead          bool                 `json:"native_read"`
	NativeReadAt        int64                `json:"native_read_at"`
	NativeReadReceipt   string               `json:"native_read_receipt_id"`
	NativeConsumed      bool                 `json:"native_consumed"`
	MissionApplied      bool                 `json:"mission_applied"`
	SourceCompletedAt   int64                `json:"source_completed_at"`
	FirstObservedAt     int64                `json:"first_observed_at"`
	CommittedAt         int64                `json:"mailbox_committed_at"`
	ProcessedAt         int64                `json:"processed_at"`
}
type Member struct {
	DispatchID     string `json:"dispatch_id"`
	Request        string `json:"request_id"`
	Mode           string `json:"mode"`
	IntentHash     string `json:"intent_hash"`
	AliasProven    bool   `json:"alias_proven"`
	LegacyDelivery string `json:"legacy_delivery"`
	CreatedAt      int64  `json:"created_at"`
}
type Observation struct {
	Scope             Scope
	Execution         Execution
	Member            Member
	State             string
	Final             *Final
	Verified          bool
	Historical        bool
	LegacyOutstanding bool
	CompletedAt       int64
}
type Control struct {
	Revision      uint64 `json:"revision"`
	Mode          string `json:"mode"`
	Paused        bool   `json:"paused"`
	Deadline      int64  `json:"deadline_ms"`
	MaxBatches    int    `json:"max_batches"`
	MaxNoProgress int    `json:"max_no_progress"`
	Issued        int    `json:"issued_batches"`
	NoProgress    int    `json:"no_progress"`
	SourceIntent  string `json:"source_intent"`
}
type ReadProof struct {
	Principal  string `json:"principal"`
	Request    string `json:"request_id"`
	Turn       string `json:"native_turn_id"`
	Invocation string `json:"invocation"`
	Native     bool   `json:"native"`
}
type ReadReceipt struct {
	ID        string    `json:"read_receipt_id"`
	ScopeHash string    `json:"scope_hash"`
	Versions  []Version `json:"versions"`
	Native    bool      `json:"native"`
	CreatedAt int64     `json:"created_at"`
}
type Page struct {
	Kind        string      `json:"kind"`
	Groups      []Group     `json:"groups"`
	ReadReceipt ReadReceipt `json:"read_receipt"`
	HighWater   uint64      `json:"high_water"`
	NextCursor  string      `json:"next_cursor"`
	HasMore     bool        `json:"has_more"`
	Complete    bool        `json:"complete"`
	Unread      int         `json:"unread_count"`
	Control     Control     `json:"control"`
}
type MembersPage struct {
	Kind       string   `json:"kind"`
	Group      Group    `json:"group"`
	Members    []Member `json:"members"`
	NextCursor string   `json:"next_cursor"`
	HasMore    bool     `json:"has_more"`
	Complete   bool     `json:"complete"`
}
type Batch struct {
	ID              string    `json:"batch_id"`
	Request         string    `json:"request_id"`
	Parent          Identity  `json:"parent"`
	ControlRevision uint64    `json:"control_revision"`
	Versions        []Version `json:"versions"`
	State           string    `json:"state"`
	Payload         string    `json:"payload"`
	PayloadHash     string    `json:"payload_hash"`
	HighWater       uint64    `json:"high_water"`
	NativeTurn      string    `json:"native_turn_id"`
	CreatedAt       int64     `json:"created_at"`
}
type NativeProvenance struct {
	Source      Identity `json:"source"`
	Request     string   `json:"request_id"`
	Turn        string   `json:"native_turn_id"`
	CallID      string   `json:"tool_call_id"`
	ReadReceipt string   `json:"read_receipt_id"`
}
type Receipt struct {
	NativeProvenance *NativeProvenance `json:"native_provenance,omitempty"`
	Kind             string            `json:"kind"`
	Intent           string            `json:"intent_id"`
	Operation        string            `json:"operation"`
	InputHash        string            `json:"input_hash"`
	State            string            `json:"state"`
	Versions         []Version         `json:"versions"`
	Control          *Control          `json:"control,omitempty"`
}
type Change struct {
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"change_kind"`
}
type Events struct {
	Kind           string   `json:"kind"`
	Changes        []Change `json:"changes"`
	HighWater      uint64   `json:"high_water"`
	RetentionFloor uint64   `json:"retention_floor"`
	HasMore        bool     `json:"has_more"`
	Complete       bool     `json:"complete"`
}
type Status struct {
	ObservedAt int64   `json:"observed_at"`
	Kind       string  `json:"kind"`
	Control    Control `json:"control"`
	Batches    []Batch `json:"batches"`
	HighWater  uint64  `json:"high_water"`
	Unread     int     `json:"unread_count"`
	WakeReason string  `json:"wake_reason"`
}

func TextHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

type MissionApplication struct {
	RecordID           string   `json:"record_id"`
	DefinitionRevision uint64   `json:"definition_revision"`
	CheckpointRevision uint64   `json:"checkpoint_revision"`
	EventID            string   `json:"event_id"`
	ContinuationID     string   `json:"continuation_id"`
	IntentID           string   `json:"intent_id"`
	InputHash          string   `json:"input_hash"`
	Processing         Identity `json:"processing_source"`
	ProcessingRequest  string   `json:"processing_request_id"`
	GroupVersion       uint64   `json:"group_version"`
	FactHash           string   `json:"fact_hash"`
}
