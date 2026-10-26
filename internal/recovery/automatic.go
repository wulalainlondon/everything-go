package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

const AutomaticMode = "network_auto_continue"
const MaxInputBytes = 256 << 20

// These are proven pre-writer scheduling outcomes, not provider failures or
// unknown acceptance. No new dispatch tombstone exists for either outcome.
var ErrDispatchDeferred = errors.New("automatic recovery scheduling deferred")
var ErrDispatchPaused = errors.New("automatic recovery scheduling budget exhausted")

var ErrGuard = errors.New("automatic recovery ownership or evidence changed")

// AutomaticSource is a private immutable admission snapshot, never a credential.
type AutomaticSource struct {
	SourceRevision       uint64 `json:"source_revision"`
	ControlRevision      uint64 `json:"control_revision"`
	CredentialGeneration string `json:"credential_generation,omitempty"`
	Provider             string `json:"provider_subject_hash,omitempty"`
	Authority            string `json:"authority"`
	Session              string `json:"session_id"`
	Request              string `json:"request_id"`
	Thread               string `json:"thread_id"`
	Configuration        string `json:"configuration_hash"`
	Principal            string `json:"principal_hash"`
	Scope                string `json:"scope_hash"`
}
type Run struct {
	Source            AutomaticSource `json:"source"`
	Revision          uint64          `json:"revision"`
	Generation        uint64          `json:"generation"`
	OwnerEpoch        string          `json:"owner_epoch"`
	State             string          `json:"state"`
	Reason            string          `json:"reason,omitempty"`
	InputHash         string          `json:"input_hash"`
	LeaseUntil        int64           `json:"input_lease_until"`
	Attempts          int             `json:"attempts"`
	MaxAttempts       int             `json:"max_attempts"`
	FirstFailureAt    int64           `json:"first_failure_at,omitempty"`
	Deadline          int64           `json:"deadline,omitempty"`
	NextAt            int64           `json:"next_at,omitempty"`
	NoProgress        int             `json:"no_progress"`
	ProgressHash      string          `json:"progress_hash,omitempty"`
	LastAttempt       string          `json:"last_attempt_id,omitempty"`
	ParentTurn        string          `json:"parent_turn_id,omitempty"`
	Kind              string          `json:"kind,omitempty"`
	NativeStatus      string          `json:"native_status,omitempty"`
	ObjectiveVerified bool            `json:"objective_verified"`
}
type Attempt struct {
	EffectsSettled bool       `json:"effects_settled"`
	Kind           string     `json:"kind,omitempty"`
	ID             string     `json:"attempt_id"`
	Number         int        `json:"number"`
	Generation     uint64     `json:"generation"`
	State          string     `json:"state"`
	Thread         string     `json:"thread_id"`
	Turn           string     `json:"turn_id,omitempty"`
	TransportEpoch uint64     `json:"transport_epoch"`
	InputHash      string     `json:"input_hash"`
	Failure        Failure    `json:"failure"`
	Acceptance     Acceptance `json:"acceptance"`
	Terminal       bool       `json:"terminal"`
	EvidenceHash   string     `json:"evidence_hash,omitempty"`
}

// CanonicalEvidence is adapter-produced proof. Opaque/unsupported tools cannot
// establish side-effect safety by returning text that says they are safe.
type CanonicalEvidence struct {
	Thread, Turn, Status string
	Complete             bool
	Idle                 bool
	ToolCoverage         bool
	NoUnresolvedEffects  bool
	ProgressHash         string
	EvidenceHash         string
	WaitingUser          bool
}
type AutomaticOutcome struct {
	Failure         Failure
	Acceptance      Acceptance
	IngressRejected bool
	Terminal        bool
	NativeWillRetry *bool
	Thread, Turn    string
	RetryAfter      time.Duration
	Canonical       CanonicalEvidence
}
type AutomaticNext struct {
	Attempt      Attempt
	Continuation bool
	ParentTurn   string
}

// AutomaticController is the core-owned durable coordinator for one admitted
// logical job. An executor never acquires authority from a recovery prompt.
type AutomaticController interface {
	Resume() (Run, error)
	BindThread(string) error
	BindProvider(string) error
	BeforeDispatch(context.Context, string, uint64) (AutomaticNext, error)
	Accepted(string, string, string) error
	Failed(AutomaticOutcome) (bool, error)
	NativeFinished(string) error
	ObserveFailure(AutomaticOutcome) error
	Guard() error
	WriterGuard() (func(), error)
	VerifyInputs() error
	Wait(context.Context) error
}
type automaticKey struct{}

func WithAutomatic(ctx context.Context, c AutomaticController) context.Context {
	return context.WithValue(ctx, automaticKey{}, c)
}
func Automatic(ctx context.Context) AutomaticController {
	c, _ := ctx.Value(automaticKey{}).(AutomaticController)
	return c
}
func Hash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func Recoverable(o AutomaticOutcome) (string, string) {
	switch o.Failure.Category {
	case PermissionRevoked, PolicyBlocked, AuthenticationRequired, UsageExhausted, OwnershipConflict, ConfigurationError:
		return "", string(o.Failure.Category)
	}
	if o.NativeWillRetry != nil && *o.NativeWillRetry {
		return "", "native_retry_in_progress"
	}
	if o.Acceptance == Rejected && o.IngressRejected && !o.Terminal && o.Failure.Source == "codex_ingress" && o.Failure.Category == ModelCapacity {
		return "retry_input", "verified_ingress_rejection"
	}
	if o.Acceptance != Accepted || !o.Terminal {
		return "", "acceptance_unknown"
	}
	if o.Failure.Source != "codex_error_info" || (o.Failure.Category != TransportUncertain && o.Failure.Category != ModelCapacity && o.Failure.Category != TemporaryRateLimit) {
		return "", "unsupported_terminal_failure"
	}
	p := o.Canonical
	if p.WaitingUser {
		return "", "waiting_user"
	}
	if !p.Complete || !p.Idle || !p.ToolCoverage || !p.NoUnresolvedEffects || p.Thread != o.Thread || p.Turn != o.Turn || p.Status != "failed" || p.EvidenceHash == "" {
		return "", "canonical_or_side_effect_evidence_unavailable"
	}
	return "continue", "exact_failed_turn_reconciled"
}
func Delay(number int, jitter float64, retryAfter time.Duration) time.Duration {
	seconds := []int{10, 30, 60, 120, 300}
	if number < 1 {
		number = 1
	}
	if number > len(seconds) {
		number = len(seconds)
	}
	if jitter < -0.2 {
		jitter = -.2
	}
	if jitter > .2 {
		jitter = .2
	}
	d := time.Duration(float64(time.Duration(seconds[number-1])*time.Second) * (1 + jitter))
	if retryAfter > d {
		d = retryAfter
	}
	return d
}
