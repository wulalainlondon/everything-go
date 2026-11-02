package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type NativeAsyncBatch interface {
	PrepareAsyncBatch(*session.Session, protocol.AsyncBatchRequest) (string, error)
}
type NativeAsyncBatchReservation interface {
	ReserveAsyncBatch(*session.Session, protocol.AsyncBatchRequest) (string, func(), error)
}
type AsyncBatchBinding struct {
	OwnerDevice     string                     `json:"owner_device"`
	OwnerGeneration uint64                     `json:"owner_generation"`
	Cwd             string                     `json:"cwd"`
	SourceRevision  uint64                     `json:"source_revision"`
	Request         protocol.AsyncBatchRequest `json:"request"`
	Configuration   session.Configuration      `json:"configuration"`
	ContentHash     string                     `json:"content_hash"`
}
type asyncBatchKey struct{}

func WithAsyncBatch(ctx context.Context, b *AsyncBatchBinding) context.Context {
	return context.WithValue(ctx, asyncBatchKey{}, b)
}
func AsyncBatchFrom(ctx context.Context) *AsyncBatchBinding {
	v, _ := ctx.Value(asyncBatchKey{}).(*AsyncBatchBinding)
	return v
}
func AsyncContentHash(content string) string {
	v := sha256.Sum256([]byte(content))
	return hex.EncodeToString(v[:])
}

// Stable canonical descriptor excludes arrival/status timestamps. Scope changes
// change the revision; a bare request ID never retires a different descriptor.
func AsyncQuestionRevision(p protocol.UserInputRequestPayload, thread string, revision uint64) string {
	p.CreatedAt = 0
	p.Status = ""
	p.QuestionRevision = ""
	p.NativeThreadID = ""
	p.ConfigRevision = 0
	p.Revision = 0
	p.Generation = ""
	raw, _ := json.Marshal(struct {
		Payload  protocol.UserInputRequestPayload `json:"payload"`
		Thread   string                           `json:"thread"`
		Revision uint64                           `json:"config_revision"`
	}{p, thread, revision})
	return AsyncContentHash(string(raw))
}

// Native question descriptors also bind the complete active configuration, so
// a same-revision runtime model/effort observation cannot silently retarget it.
func AsyncQuestionRevisionForConfiguration(p protocol.UserInputRequestPayload, thread string, config session.Configuration) string {
	descriptor := AsyncQuestionRevision(p, thread, config.Revision)
	raw, _ := json.Marshal(struct {
		Descriptor    string                `json:"descriptor"`
		Configuration session.Configuration `json:"configuration"`
	}{descriptor, config})
	return AsyncContentHash(string(raw))
}

// Internal canonical-native observation. Hub consumes it, never forwards answer
// text or treats queue admission as consumption.
type NativeAsyncBatchConsumed struct{ SessionID, ThreadID, ContentHash string }
