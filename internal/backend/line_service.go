package backend

import (
	"context"
	"encoding/json"
)

type LineServiceProvider interface {
	LineServiceEnabled() bool
	ExecuteLineTool(context.Context, SessionControlCaller, string, json.RawMessage) (any, error)
}
