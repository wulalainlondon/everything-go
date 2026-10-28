package taskapi

import (
	"errors"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"strconv"
)

// These facts contain only fixed schema categories, closed keywords and bounded
// array positions. Never format ErrorKind, the error string or instance values.
type closedSchemaWitness struct {
	Path    string `json:"path"`
	Keyword string `json:"keyword"`
	Row     int    `json:"row"`
	Element int    `json:"element"`
}

func closedReadSchemaWitness(err error) []closedSchemaWitness {
	unknown := closedSchemaWitness{"schema_unknown", "other", -1, -1}
	var root *jsonschema.ValidationError
	if !errors.As(err, &root) || root == nil {
		return []closedSchemaWitness{unknown}
	}
	const maxNodes = 8192
	pending := []*jsonschema.ValidationError{root}
	seen := map[closedSchemaWitness]bool{}
	out := []closedSchemaWitness{}
	visited := 0
	for len(pending) > 0 && visited < maxNodes && len(out) < 4 {
		node := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		visited++
		if node == nil {
			continue
		}
		if len(node.Causes) > 0 {
			// Total scheduled visits, not only depth, is bounded even for a huge tree.
			remaining := maxNodes - visited - len(pending)
			count := len(node.Causes)
			if count > remaining {
				count = remaining
			}
			for i := count - 1; i >= 0; i-- {
				pending = append(pending, node.Causes[i])
			}
			continue
		}
		fact := unknown
		p := node.InstanceLocation
		if node.ErrorKind != nil && len(p) >= 4 && p[0] == "result" && p[1] == "items" {
			row, valid := schemaPosition(p[2])
			kw := node.ErrorKind.KeywordPath()
			if valid && len(kw) > 0 && closedSchemaKeyword(kw[0]) {
				path := ""
				element := -1
				switch {
				case len(p) == 5 && p[3] == "source" && p[4] == "binding_kind":
					path = "result.items[].source.binding_kind"
				case len(p) == 5 && p[3] == "receipt_ids":
					if n, ok := schemaPosition(p[4]); ok {
						element = n
						path = "result.items[].receipt_ids[]"
					}
				case len(p) == 4 && p[3] == "goal":
					path = "result.items[].goal"
				case len(p) == 6 && p[3] == "source" && p[4] == "identity":
					switch p[5] {
					case "instance_id", "session_id", "request_id", "native_thread_id", "native_turn_id", "config_revision":
						path = "result.items[].source.identity." + p[5]
					}
				case len(p) == 5 && p[3] == "axes":
					switch p[4] {
					case "admission", "execution", "final", "artifact", "qa", "delivery", "cancellation":
						path = "result.items[].axes." + p[4]
					}
				}
				if path != "" {
					fact = closedSchemaWitness{path, kw[0], row, element}
				}
			}
		}
		if !seen[fact] {
			seen[fact] = true
			out = append(out, fact)
		}
	}
	if len(out) == 0 {
		return []closedSchemaWitness{unknown}
	}
	return out
}
func schemaPosition(value string) (int, bool) {
	n, err := strconv.Atoi(value)
	return n, err == nil && n >= 0 && n < 100 && strconv.Itoa(n) == value
}
func closedSchemaKeyword(value string) bool {
	switch value {
	case "enum", "minLength", "maxLength", "required", "additionalProperties", "type", "minimum", "maximum", "const", "pattern":
		return true
	}
	return false
}
