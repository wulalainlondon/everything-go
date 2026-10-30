package backend

// ContextTelemetry is internal provider evidence, not an inbound app command.
// Only native root callbacks may construct it. Completion aliases join one
// managed operation's item and maintenance terminal without counting twice.
type ContextTelemetry struct {
	SessionID, Backend, NativeGeneration string
	ReportedAtMS                         int64
	SourceRevision                       uint64
	EventID                              string
	Origin, MaxOrigin                    string
	UsedTokens, MaxTokens                *int
	Completed                            bool
	CompletionState                      string
	Aliases                              []string
}
