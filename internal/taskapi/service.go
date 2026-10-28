package taskapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"unicode/utf8"

	taskcontract "everything-go/contracts/task-api/v1"
)

type Service struct {
	contract      *taskcontract.Contract
	mediaContract *taskcontract.Contract
	policy        Authorizer
	gateway       Gateway
}

func NewService(policy Authorizer, gateway Gateway) (*Service, error) {
	c, err := taskcontract.New()
	if err != nil {
		return nil, err
	}
	media, err := taskcontract.NewMedia()
	if err != nil {
		return nil, err
	}
	return &Service{contract: c, mediaContract: media, policy: policy, gateway: gateway}, nil
}

var operations = map[string]bool{"capabilities": true, "intake": true, "create_dispatch": true, "list": true, "get": true, "read_result": true, "append": true, "cancel": true, "snapshot": true, "events": true, "deliver_result": true, "read_input": true, "legacy_source": true}

func isMutation(op string) bool {
	return op == "create_dispatch" || op == "append" || op == "cancel" || op == "deliver_result"
}

func (s *Service) Execute(ctx context.Context, caller BoundCaller, raw []byte) Response {
	value, err := taskcontract.Parse(raw)
	response := Response{Type: "task_api_response", Version: taskcontract.Version, CorrelationID: "invalid-frame"}
	header, _ := value.(map[string]any)
	if text, ok := header["correlation_id"].(string); ok && len(text) > 0 && utf8.RuneCountInString(text) <= 512 {
		response.CorrelationID = text
	}
	if text, ok := header["operation"].(string); ok && len(text) > 0 && utf8.RuneCountInString(text) <= 512 {
		response.Operation = &text
	}
	if text, ok := header["api_version"].(string); ok && len(text) > 0 && utf8.RuneCountInString(text) <= 128 {
		response.RequestVersion = &text
	}
	fail := func(e error) Response {
		var apiErr *APIError
		if !errors.As(e, &apiErr) {
			apiErr = Failure("unknown_acceptance", "unknown", "lookup_original")
		}
		response.Error = apiErr
		return response
	}
	if err != nil {
		return fail(Failure("invalid_argument", "known_none", "correct_input"))
	}
	binding, err := caller.Check(ctx)
	if err != nil {
		return fail(err)
	}
	validator := s.contract
	switch binding.TaskSchemaHash {
	case "", taskcontract.Hash():
	case taskcontract.MediaHash():
		validator = s.mediaContract
	default:
		return fail(Failure("unsupported", "known_none", "read_capabilities"))
	}
	ctx = withTaskSchema(ctx, binding.TaskSchemaHash)
	if response.Operation == nil || response.RequestVersion == nil {
		return fail(Failure("invalid_argument", "known_none", "correct_input"))
	}
	if *response.RequestVersion != taskcontract.Version || !operations[*response.Operation] {
		return fail(Failure("unsupported", "known_none", "read_capabilities"))
	}
	if err = validator.ValidateValue(value, "Request"); err != nil {
		return fail(Failure("invalid_argument", "known_none", "correct_input"))
	}
	var request Request
	if err = json.Unmarshal(raw, &request); err != nil {
		return fail(Failure("invalid_argument", "known_none", "correct_input"))
	}
	if request.Operation == "deliver_result" && !binding.InternalDelivery {
		return fail(Failure("permission", "known_none", "request_scope_change"))
	}
	if request.Operation == "create_dispatch" {
		input, parseErr := taskcontract.Parse(request.Input)
		if parseErr != nil {
			return fail(Failure("invalid_argument", "known_none", "correct_input"))
		}
		if worker, ok := input.(map[string]any)["new_worker"].(map[string]any); ok {
			if _, exists := worker["profile"]; !exists {
				worker["profile"] = map[string]any{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}
			}
		}
		request.Input, _ = json.Marshal(input) // Persist effective default, never parent inheritance.
	}
	if s.policy == nil || s.gateway == nil {
		return fail(Failure("unsupported", "known_none", "read_capabilities"))
	}
	locator, err := s.policy.Authorize(ctx, binding, request)
	if err != nil {
		return fail(err)
	}
	command := AuthorizedCommand{Caller: binding, Request: request, Locator: locator, Revalidate: func(ctx context.Context) error { _, err := caller.Check(ctx); return err }}
	if request.Operation == "get" {
		var input struct {
			Original *Locator `json:"original_receipt"`
		}
		if err := json.Unmarshal(request.Input, &input); err != nil {
			return fail(Failure("invalid_argument", "known_none", "correct_input"))
		}
		if input.Original != nil {
			asserted := *input.Original
			if asserted.Authority != binding.Authority || locator.Authority != binding.Authority {
				return fail(Failure("wrong_authority", "known_none", "refresh_identity"))
			}
			if asserted != locator {
				return fail(Failure("permission", "known_none", "request_scope_change"))
			}
			command.Namespace = Namespace{binding.Authority, binding.StableScopeID, binding.NamespaceGeneration, locator.Path, locator.Operation, locator.TaskID}
		}
	}
	if mutationRequest(request) {
		namespace, err := MakeNamespace(binding, locator, request)
		if err != nil {
			return fail(err)
		}
		command.Namespace = namespace
		command.IntentHash, err = IntentHash(request)
		if err != nil {
			return fail(Failure("invalid_argument", "known_none", "correct_input"))
		}
	}
	// Revalidate immediately before the owner gateway. It must revalidate under
	// its own transaction/handoff policy as well; this check is not a lease.
	if _, err = caller.Check(ctx); err != nil {
		return fail(err)
	}
	var result any
	if mutationRequest(request) {
		result, err = s.gateway.Mutate(ctx, command)
	} else {
		result, err = s.gateway.Read(ctx, command)
	}
	if err != nil {
		readDiagnostic(request, binding.TaskSchemaHash, "gateway_read", err, 0)
		return fail(err)
	}
	// Gateway reads may perform slow canonical I/O. A revoked invocation never
	// receives result contents through its previously valid transport binding.
	if _, err = caller.Check(ctx); err != nil {
		if mutationRequest(request) {
			return fail(Failure("caller_unbound", "known_receipt", "lookup_original"))
		}
		return fail(Failure("caller_unbound", "known_none", "refresh_identity"))
	}
	if mutationRequest(request) {
		encoded, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return fail(Failure("unknown_acceptance", "unknown", "lookup_original"))
		}
		var receipt struct {
			Locator   Locator `json:"locator"`
			Operation string  `json:"operation"`
			Key       string  `json:"idempotency_key"`
			Hash      string  `json:"intent_hash"`
			TaskID    string  `json:"task_id"`
			Ref       struct {
				Authority string `json:"authority_instance_id"`
				Path      string `json:"path"`
			} `json:"task_ref"`
		}
		if json.Unmarshal(encoded, &receipt) != nil || receipt.Locator != command.Locator || receipt.Operation != request.Operation || receipt.Key != request.IdempotencyKey || receipt.Hash != command.IntentHash || receipt.Ref.Authority != binding.Authority || receipt.Ref.Path != command.Namespace.Path || (command.Namespace.TaskID != "" && receipt.TaskID != command.Namespace.TaskID) {
			return fail(Failure("unknown_acceptance", "unknown", "lookup_original"))
		}
	}
	response.OK = true
	response.Result = result
	response.RequestVersion = nil
	encoded, marshalErr := json.Marshal(response)
	validationErr := marshalErr
	if marshalErr != nil {
		readDiagnostic(request, binding.TaskSchemaHash, "response_marshal", marshalErr, 0)
	} else {
		validationErr = validator.Validate(encoded, "Response")
		if validationErr != nil {
			readDiagnostic(request, binding.TaskSchemaHash, "response_validate", validationErr, len(encoded))
		}
	}
	if validationErr != nil {
		response.OK = false
		response.Result = nil
		response.RequestVersion = &request.Version
		return fail(Failure("unknown_acceptance", "unknown", "lookup_original"))
	}
	return response
}

// Only these two read routes emit bounded technical diagnostics. Never format
// an error, input, result, identity or raw correlation: validators and drivers
// may include private values in their error strings.
func readDiagnostic(request Request, schema, stage string, err error, size int) {
	if request.Operation != "list" && request.Operation != "snapshot" {
		return
	}
	if stage != "gateway_read" && stage != "response_marshal" && stage != "response_validate" {
		return
	}
	reason := "other"
	switch {
	case errors.Is(err, context.Canceled):
		reason = "context_cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		reason = "context_deadline"
	case stage == "response_validate" && size > taskcontract.MaxBytes:
		reason = "frame_bound"
	case stage == "response_validate":
		reason = "schema"
	}
	if schema != taskcontract.MediaHash() {
		schema = taskcontract.Hash()
	}
	digest := sha256.Sum256([]byte(request.CorrelationID))
	witness := ""
	if stage == "response_validate" && reason == "schema" {
		encoded, _ := json.Marshal(closedReadSchemaWitness(err))
		witness = " schema_witness=" + string(encoded)
	}
	log.Printf("task_api_read_diagnostic correlation_sha256=%x operation=%s stage=%s reason=%s response_bytes=%d contract_sha256=%s%s", digest, request.Operation, stage, reason, size, schema, witness)
}

func MakeNamespace(c VerifiedContext, l Locator, r Request) (Namespace, error) {
	if l.Authority != c.Authority {
		return Namespace{}, Failure("wrong_authority", "known_none", "refresh_identity")
	}
	if l.Operation != r.Operation || l.Key != r.IdempotencyKey || !knownPath(l.Path) {
		return Namespace{}, Failure("invalid_argument", "known_none", "correct_input")
	}
	var input map[string]any
	if err := json.Unmarshal(r.Input, &input); err != nil {
		return Namespace{}, err
	}
	if r.Operation == "create_dispatch" || r.Operation == "intake" || r.Operation == "legacy_source" {
		if l.TaskID != "" {
			return Namespace{}, Failure("invalid_argument", "known_none", "correct_input")
		}
	} else {
		task, _ := input["task_id"].(string)
		if task == "" || task != l.TaskID {
			return Namespace{}, Failure("invalid_argument", "known_none", "correct_input")
		}
	}
	return Namespace{c.Authority, c.StableScopeID, c.NamespaceGeneration, l.Path, r.Operation, l.TaskID}, nil
}
func knownPath(path string) bool {
	return path == "ordinary" || path == "controller" || path == "delegation" || path == "pm_v1" || path == "pm_v2"
}

// Key returns an unambiguous tuple encoding, not separator concatenation.
func (n Namespace) Key(key string) string {
	raw, _ := json.Marshal([]any{n.Authority, n.StableScopeID, n.Generation, n.Path, n.Operation, n.TaskID, key})
	return string(raw)
}

// IntentHash excludes transport/correlation/tool call and process generation.
// Defaults are a versioned, fixed new-worker profile, never root inheritance.
func IntentHash(r Request) (string, error) {
	input, err := taskcontract.Parse(r.Input)
	if err != nil {
		return "", err
	}
	obj, ok := input.(map[string]any)
	if !ok {
		return "", errors.New("input is not an object")
	}
	if r.Operation == "create_dispatch" {
		if worker, ok := obj["new_worker"].(map[string]any); ok {
			if _, exists := worker["profile"]; !exists {
				worker["profile"] = map[string]any{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}
			}
		}
	}
	normalized, err := normalizeNumbers(obj)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal([]any{r.Version, r.Operation, normalized})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func normalizeNumbers(value any) (any, error) {
	switch x := value.(type) {
	case json.Number:
		number, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return nil, err
		}
		return json.Number(strconv.FormatFloat(number, 'g', -1, 64)), nil
	case map[string]any:
		out := map[string]any{}
		for key, item := range x {
			v, err := normalizeNumbers(item)
			if err != nil {
				return nil, err
			}
			out[key] = v
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for index, item := range x {
			v, err := normalizeNumbers(item)
			if err != nil {
				return nil, err
			}
			out[index] = v
		}
		return out, nil
	default:
		return value, nil
	}
}

func mutationRequest(r Request) bool {
	if r.Operation == "intake" {
		var input struct{ Action string }
		json.Unmarshal(r.Input, &input)
		return input.Action == "submit"
	}
	if r.Operation != "legacy_source" {
		return isMutation(r.Operation)
	}
	var input struct{ Action string }
	json.Unmarshal(r.Input, &input)
	return input.Action == "confirm" || input.Action == "revoke"
}

type taskSchemaContextKey struct{}

func withTaskSchema(ctx context.Context, hash string) context.Context {
	return context.WithValue(ctx, taskSchemaContextKey{}, hash)
}

// MediaTaskView only changes the wire projection; canonical task data is never rewritten.
func MediaTaskView(ctx context.Context) bool {
	hash, _ := ctx.Value(taskSchemaContextKey{}).(string)
	return hash == taskcontract.MediaHash()
}
