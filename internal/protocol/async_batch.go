package protocol

// AsyncBatchSelection freezes the exact server-issued descriptor and explicit
// human answer mapping. It is never an approval or a delegated-task answer.
type AsyncBatchSelection struct {
	RequestID        string         `json:"request_id"`
	QuestionRevision string         `json:"question_revision"`
	Answers          map[string]any `json:"answers"`
}
type AsyncBatchRequest struct {
	Authority      string                `json:"authority_instance_id"`
	SessionID      string                `json:"session_id"`
	RequestID      string                `json:"request_id"`
	ThreadID       string                `json:"expected_native_thread_id"`
	ConfigRevision uint64                `json:"expected_config_revision"`
	Selected       []AsyncBatchSelection `json:"selected"`
}
type AsyncCoveredQuestion struct {
	RequestID        string `json:"request_id"`
	QuestionRevision string `json:"question_revision"`
}
type AsyncAdmission struct {
	SessionID        string `json:"session_id"`
	RequestID        string `json:"request_id"`
	QuestionRevision string `json:"question_revision"`
	ReplyRequestID   string `json:"reply_request_id"`
	State            string `json:"state"`
}
type AsyncBatchReceipt struct {
	Type          string                 `json:"type"`
	Authority     string                 `json:"authority_instance_id"`
	SessionID     string                 `json:"session_id"`
	RequestID     string                 `json:"request_id"`
	Phase         string                 `json:"phase"`
	QueueState    string                 `json:"queue_state,omitempty"`
	KnownNoEffect bool                   `json:"known_no_effect"`
	ErrorCode     string                 `json:"error_code,omitempty"`
	Covered       []AsyncCoveredQuestion `json:"covered"`
	Revision      uint64                 `json:"revision"`
	Generation    string                 `json:"snapshot_generation"`
}
