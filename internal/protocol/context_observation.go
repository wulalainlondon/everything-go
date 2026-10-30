package protocol

// ContextObservation is a current physical conversation's last provider report,
// never a claim of live per-token measurement or complete pre-tracking history.
type ContextObservation struct {
	SchemaVersion       int                     `json:"schema_version"`
	AuthorityInstanceID string                  `json:"authority_instance_id"`
	SessionID           string                  `json:"session_id"`
	Backend             string                  `json:"backend"`
	NativeGeneration    string                  `json:"native_generation"`
	SourceRevision      uint64                  `json:"source_revision"`
	Usage               ContextUsageObservation `json:"usage"`
	Compactions         ContextCompactions      `json:"compactions"`
}
type ContextUsageObservation struct {
	UsedTokens   *int    `json:"used_tokens"`
	MaxTokens    *int    `json:"max_tokens"`
	Origin       *string `json:"origin"`
	MaxOrigin    *string `json:"max_origin"`
	ReportedAtMS *int64  `json:"reported_at_ms"`
	FreshUntilMS *int64  `json:"fresh_until_ms"`
	Status       string  `json:"status"`
}
type ContextCompactions struct {
	Completed      *int   `json:"completed"`
	Coverage       string `json:"coverage"`
	TrackedSinceMS *int64 `json:"tracked_since_ms"`
	AsOfMS         int64  `json:"as_of_ms"`
	Supported      bool   `json:"supported"`
}
type ContextObservationEvent struct {
	Type                string             `json:"type"`
	SessionID           string             `json:"session_id"`
	AuthorityInstanceID string             `json:"authority_instance_id"`
	ContextObservation  ContextObservation `json:"context_observation"`
}
