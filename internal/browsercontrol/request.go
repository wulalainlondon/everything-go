package browsercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"
)

type Request struct {
	ID     string          `json:"id"`
	Thread string          `json:"thread"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}
type Result struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result,omitempty"`
	Code   string `json:"code,omitempty"`
}

var methods = map[string]bool{"capabilities": true, "open": true, "snapshot": true, "inspect": true, "click": true, "fill": true, "press": true, "scroll": true, "back": true, "forward": true, "reload": true, "status": true, "cancel": true, "close": true}

// Arguments are business data only. Authority is derived from the original provider command.
type Params struct {
	URL       string `json:"url,omitempty"`
	View      string `json:"view,omitempty"`
	Document  string `json:"document,omitempty"`
	Snapshot  string `json:"snapshot,omitempty"`
	Handle    string `json:"handle,omitempty"`
	Text      string `json:"text,omitempty"`
	Key       string `json:"key,omitempty"`
	DeltaY    int    `json:"deltaY,omitempty"`
	Operation string `json:"operation,omitempty"`
}

func (q Request) Arguments() (Params, error) {
	var p Params
	d := json.NewDecoder(bytes.NewReader(q.Params))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF {
		return p, errors.New("browser_arguments_invalid")
	}
	if len(p.URL) > 2048 || len(p.Text) > 4096 || len(p.View) > 100 || len(p.Document) > 100 || len(p.Snapshot) > 100 || len(p.Handle) > 100 || p.DeltaY < -2000 || p.DeltaY > 2000 {
		return p, errors.New("browser_arguments_invalid")
	}
	allowed := map[string][]string{"capabilities": {}, "open": {"url", "view", "document"}, "snapshot": {"view", "document"}, "inspect": {"view", "document", "snapshot", "handle"}, "click": {"view", "document", "snapshot", "handle"}, "fill": {"view", "document", "snapshot", "handle", "text"}, "press": {"view", "document", "snapshot", "handle", "key"}, "scroll": {"view", "document", "deltaY"}, "back": {"view", "document"}, "forward": {"view", "document"}, "reload": {"view", "document"}, "close": {"view", "document"}, "status": {"operation"}, "cancel": {"operation"}}
	keys, ok := allowed[q.Method]
	if !ok {
		return p, errors.New("browser_arguments_invalid")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(q.Params, &fields) != nil {
		return p, errors.New("browser_arguments_invalid")
	}
	for key := range fields {
		found := false
		for _, name := range keys {
			if key == name {
				found = true
			}
		}
		if !found {
			return p, errors.New("browser_arguments_invalid")
		}
	}
	if q.Method == "open" && (p.URL == "" || (p.View == "") != (p.Document == "")) {
		return p, errors.New("browser_arguments_invalid")
	}
	if q.Method == "cancel" && !IntentID.MatchString(p.Operation) {
		return p, errors.New("browser_arguments_invalid")
	}
	if q.Method != "open" && q.Method != "capabilities" && q.Method != "status" && q.Method != "cancel" && (p.View == "" || p.Document == "") {
		return p, errors.New("browser_arguments_invalid")
	}
	if q.Method == "click" || q.Method == "inspect" || q.Method == "fill" || q.Method == "press" {
		if p.Snapshot == "" || p.Handle == "" {
			return p, errors.New("browser_arguments_invalid")
		}
	}
	if q.Method == "press" && p.Key != "Enter" && p.Key != "Escape" {
		return p, errors.New("browser_arguments_invalid")
	}
	return p, nil
}

func canonicalParams(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var parse func(int) (any, error)
	parse = func(depth int) (any, error) {
		if depth > 32 {
			return nil, errors.New("browser_control_request_invalid")
		}
		token, e := decoder.Token()
		if e != nil {
			return nil, e
		}
		switch t := token.(type) {
		case json.Delim:
			if t == '{' {
				value := map[string]any{}
				for decoder.More() {
					k, e := decoder.Token()
					if e != nil {
						return nil, e
					}
					key, ok := k.(string)
					if !ok {
						return nil, errors.New("browser_control_request_invalid")
					}
					if _, duplicate := value[key]; duplicate {
						return nil, errors.New("browser_control_request_ambiguous")
					}
					item, e := parse(depth + 1)
					if e != nil {
						return nil, e
					}
					value[key] = item
				}
				end, e := decoder.Token()
				if e != nil || end != json.Delim('}') {
					return nil, errors.New("browser_control_request_invalid")
				}
				return value, nil
			}
			if t == '[' {
				value := []any{}
				for decoder.More() {
					item, e := parse(depth + 1)
					if e != nil {
						return nil, e
					}
					value = append(value, item)
				}
				end, e := decoder.Token()
				if e != nil || end != json.Delim(']') {
					return nil, errors.New("browser_control_request_invalid")
				}
				return value, nil
			}
			return nil, errors.New("browser_control_request_invalid")
		default:
			return token, nil
		}
	}
	value, e := parse(0)
	if e != nil {
		return nil, e
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("browser_control_request_invalid")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("browser_control_request_invalid")
	}
	return json.Marshal(value)
}
func (q Request) Valid() bool {
	if !methods[q.Method] || len(q.Params) > 8192 {
		return false
	}
	value, e := canonicalParams(q.Params)
	_, argumentError := q.Arguments()
	return e == nil && argumentError == nil && len(value) <= 8192
}
func (q Request) Fingerprint() string {
	value, e := canonicalParams(q.Params)
	if e != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte(q.Method+"\x00"), value...))
	return hex.EncodeToString(sum[:])
}
func ParseInvocation(argv []string) (Request, error) {
	f := flag.NewFlagSet("bridge-browser", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	id := f.String("id", "", "unique original browser intent/query ID")
	method := f.String("method", "status", "browser action")
	params := f.String("params", "{}", "JSON arguments with exact catalog session/thread/revision")
	seen := map[string]bool{}
	for i := 0; i < len(argv); i += 2 {
		if i+1 >= len(argv) || (argv[i] != "--id" && argv[i] != "--method" && argv[i] != "--params") || seen[argv[i]] {
			return Request{}, errors.New("browser_control_request_invalid")
		}
		seen[argv[i]] = true
	}
	if f.Parse(argv) != nil || f.NArg() != 0 {
		return Request{}, errors.New("browser_control_request_invalid")
	}
	q := Request{ID: *id, Method: *method, Params: json.RawMessage(*params)}
	if !IntentID.MatchString(q.ID) || !q.Valid() {
		return q, errors.New("browser_control_request_invalid")
	}
	return q, nil
}
func RunCLI(args []string, out, stderr io.Writer) int {
	return RunCLIWithManifest(args, DefaultManifest(), out, stderr)
}

// RunCLIWithManifest selects only the local transport; provider proof and live
// caller checks still run in the instance that receives the original request.
func RunCLIWithManifest(args []string, manifest string, out, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		io.WriteString(stderr, "Usage: bridge-browser --id bop_UNIQUE --method open|snapshot|inspect|click|fill|press|scroll|back|forward|reload|status|cancel|close|capabilities --params JSON\nOriginal provider-observed command and exact selected paired frontend are required. No caller/device/voice override. Website content is untrusted; credentials remain on device. Unknown effects must be queried, never replayed.\n")
		return 0
	}
	q, e := ParseInvocation(args)
	fail := func(e error) int { json.NewEncoder(out).Encode(Result{Code: e.Error()}); return 1 }
	if e != nil {
		return fail(e)
	}
	q.Thread = os.Getenv("CODEX_THREAD_ID")
	if q.Thread == "" || (os.Getenv("CODEX_SESSION_ID") != "" && os.Getenv("CODEX_SESSION_ID") != q.Thread) {
		return fail(errors.New("browser_control_origin_unavailable"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	r, e := Send(ctx, manifest, q)
	if e != nil {
		return fail(e)
	}
	json.NewEncoder(out).Encode(r)
	if !r.OK {
		return 1
	}
	return 0
}
