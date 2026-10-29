package backend

// ExactProviderFinal is a completed public native item, never live commentary.
// It is obtained only for an already accepted canonical Bridge request.
type ExactProviderFinal struct {
	SessionID           string   `json:"session_id"`
	RequestID           string   `json:"request_id"`
	ThreadID            string   `json:"thread_id"`
	TurnID              string   `json:"turn_id"`
	ConfigRevision      uint64   `json:"config_revision"`
	SourceRevision      uint64   `json:"source_revision"`
	SourceMessageID     string   `json:"source_message_id"`
	Text                string   `json:"text"`
	SubjectSHA256       string   `json:"subject_sha256"`
	SubjectBeforeSHA256 string   `json:"subject_before_sha256"`
	SubjectAfterSHA256  string   `json:"subject_after_sha256"`
	TransportEpoch      uint64   `json:"transport_epoch"`
	TurnResponseSHA256  string   `json:"turn_response_sha256"`
	ItemPageSHA256      []string `json:"item_page_sha256"`
}
