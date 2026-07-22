package goexec

import (
	"encoding/json"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"everything-go/internal/backend"
)

const (
	browserOriginTool = "access_browser_origin"
	elicitationChoice = "decision"
)

type browserOriginPolicy struct {
	mode           string
	allowedOrigins []string
}

func ensureBrowserElicitationRouting(codexHome string) (bool, error) {
	manage := strings.ToLower(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_MANAGE_AUTO_REVIEW")))
	switch manage {
	case "0", "false", "no", "off":
		return false, nil
	}
	if codexHome == "" {
		codexHome = os.Getenv("CODEX_HOME")
	}
	if codexHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return false, err
		}
		codexHome = filepath.Join(home, ".codex")
	}
	dir := filepath.Join(codexHome, "browser")
	path := filepath.Join(dir, "config.toml")
	original, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	lines := strings.Split(string(original), "\n")
	found := false
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "disable_auto_review") {
			if eq := strings.Index(trimmed, "="); eq >= 0 && strings.TrimSpace(trimmed[:eq]) == "disable_auto_review" {
				lines[index] = "disable_auto_review = true"
				found = true
				break
			}
		}
	}
	updated := strings.Join(lines, "\n")
	if !found {
		updated = "disable_auto_review = true\n" + string(original)
	}
	if updated == string(original) {
		return false, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, err
	}
	temporary, err := os.CreateTemp(dir, ".config.toml.*")
	if err != nil {
		return false, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err = temporary.WriteString(updated); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode().Perm()
	}
	if err := os.Chmod(temporaryPath, mode); err != nil {
		return false, err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return false, err
	}
	return true, nil
}

func browserOriginPolicyFromEnv() browserOriginPolicy {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_ORIGIN_MODE")))
	switch mode {
	case "ask", "allowlist", "allow_all", "deny":
	default:
		mode = "ask"
	}
	var allowed []string
	for _, item := range strings.Split(os.Getenv("BRIDGE_BROWSER_ALLOWED_ORIGINS"), ",") {
		if item = strings.TrimSpace(item); item != "" {
			allowed = append(allowed, item)
		}
	}
	return browserOriginPolicy{mode: mode, allowedOrigins: allowed}
}

func decodeObject(raw json.RawMessage) map[string]any {
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}

func recursiveValue(value any, key string, depth int) (any, bool) {
	if depth < 0 {
		return nil, false
	}
	switch item := value.(type) {
	case map[string]any:
		if found, ok := item[key]; ok {
			return found, true
		}
		for _, child := range item {
			if found, ok := recursiveValue(child, key, depth-1); ok {
				return found, true
			}
		}
	case []any:
		for _, child := range item {
			if found, ok := recursiveValue(child, key, depth-1); ok {
				return found, true
			}
		}
	}
	return nil, false
}

func recursiveString(value any, key string) string {
	found, _ := recursiveValue(value, key, 8)
	return strings.TrimSpace(codexAnyString(found))
}

func browserOriginRequest(params map[string]any) (tool, origin string, ok bool) {
	if recursiveString(params, "connector_id") != "browser-use" {
		return "", "", false
	}
	tool = recursiveString(params, "tool_name")
	origin = recursiveString(params, "origin")
	return tool, origin, tool != "" && origin != ""
}

func normalizedBrowserOrigin(value string) (*url.URL, string, bool) {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil {
		return nil, "", false
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	port := parsed.Port()
	if (parsed.Scheme == "http" && port == "80") || (parsed.Scheme == "https" && port == "443") {
		port = ""
	}
	hostPort := host
	if port != "" {
		hostPort = net.JoinHostPort(host, port)
	}
	normalized := parsed.Scheme + "://" + hostPort
	parsed, _ = url.Parse(normalized)
	return parsed, normalized, true
}

func hostMatches(host, pattern string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	pattern = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(pattern), "."))
	if strings.HasPrefix(pattern, "*.") {
		suffix := strings.TrimPrefix(pattern, "*.")
		return suffix != "" && host != suffix && strings.HasSuffix(host, "."+suffix)
	}
	return pattern != "" && host == pattern
}

func originMatches(origin, pattern string) bool {
	parsed, _, ok := normalizedBrowserOrigin(origin)
	if !ok {
		return false
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	if !strings.Contains(pattern, "://") {
		hostPattern, portPattern := pattern, ""
		if host, port, err := net.SplitHostPort(pattern); err == nil {
			hostPattern, portPattern = host, port
		} else if i := strings.LastIndex(pattern, ":"); i > 0 && !strings.Contains(pattern[i+1:], ":") {
			if _, err := strconv.Atoi(pattern[i+1:]); err == nil {
				hostPattern, portPattern = pattern[:i], pattern[i+1:]
			}
		}
		if !hostMatches(parsed.Hostname(), hostPattern) {
			return false
		}
		if portPattern == "" {
			return true
		}
		actualPort := parsed.Port()
		if actualPort == "" {
			if parsed.Scheme == "https" {
				actualPort = "443"
			} else {
				actualPort = "80"
			}
		}
		return actualPort == portPattern
	}
	parts, err := url.Parse(pattern)
	if err != nil || parts.Scheme != parsed.Scheme || !hostMatches(parsed.Hostname(), parts.Hostname()) {
		return false
	}
	return parts.Port() == "" || parts.Port() == parsed.Port()
}

func (p browserOriginPolicy) automaticResponse(params map[string]any) (map[string]any, bool) {
	tool, origin, ok := browserOriginRequest(params)
	if !ok || tool != browserOriginTool {
		return nil, false
	}
	if _, _, valid := normalizedBrowserOrigin(origin); !valid {
		return mcpDecision("decline", nil, nil), true
	}
	switch p.mode {
	case "deny":
		return mcpDecision("decline", nil, nil), true
	case "allow_all":
		return mcpDecision("accept", nil, map[string]any{"persist": "session"}), true
	case "allowlist":
		for _, pattern := range p.allowedOrigins {
			if originMatches(origin, pattern) {
				return mcpDecision("accept", nil, map[string]any{"persist": "session"}), true
			}
		}
	}
	return nil, false
}

func mcpDecision(action string, content any, meta any) map[string]any {
	return map[string]any{"action": action, "content": content, "_meta": meta}
}

func mcpAutomaticResponse(raw json.RawMessage) (map[string]any, bool) {
	params := decodeObject(raw)
	if params == nil {
		return nil, false
	}
	return browserOriginPolicyFromEnv().automaticResponse(params)
}

func mcpElicitationQuestions(params map[string]any) []backend.UserInputQuestion {
	tool, origin, browserRequest := browserOriginRequest(params)
	message := firstNonEmpty(codexFirstString(params, "message"), "Allow this external tool request?")
	server := firstNonEmpty(codexFirstString(params, "serverName"), "MCP request")
	if browserRequest {
		if tool != browserOriginTool {
			return []backend.UserInputQuestion{{
				QuestionID: elicitationChoice, Text: message, Header: "Chrome 高風險授權", Type: "single_select",
				Options: []backend.UserInputOption{
					{ID: "deny", Label: "拒絕", Description: origin, Recommended: true},
					{ID: "approve_once", Label: "僅允許這次", Description: origin},
				},
			}}
		}
		return []backend.UserInputQuestion{{
			QuestionID: elicitationChoice, Text: message, Header: "Chrome 網域授權", Type: "single_select",
			Options: []backend.UserInputOption{
				{ID: "approve_once", Label: "允許這次", Description: origin, Recommended: true},
				{ID: "approve_always", Label: "總是允許", Description: origin},
				{ID: "deny", Label: "拒絕", Description: origin},
			},
		}}
	}

	if codexFirstString(params, "mode") == "url" {
		text := strings.TrimSpace(message + "\n" + codexFirstString(params, "url"))
		return []backend.UserInputQuestion{{
			QuestionID: "action", Text: text, Header: server, Type: "single_select",
			Options: []backend.UserInputOption{{ID: "accept", Label: "Open / Continue", Recommended: true}, {ID: "decline", Label: "Decline"}},
		}}
	}

	schema, _ := params["requestedSchema"].(map[string]any)
	properties, _ := schema["properties"].(map[string]any)
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	questions := make([]backend.UserInputQuestion, 0, len(keys))
	for _, key := range keys {
		property, _ := properties[key].(map[string]any)
		propertyType := codexFirstString(property, "type")
		q := backend.UserInputQuestion{
			QuestionID: key,
			Text:       firstNonEmpty(codexFirstString(property, "description", "title"), key),
			Header:     firstNonEmpty(codexFirstString(property, "title"), server),
			Type:       "text",
			FreeForm:   true,
		}
		enumSchema := property
		if propertyType == "array" {
			q.Type, q.MultiSelect = "multi_select", true
			if items, ok := property["items"].(map[string]any); ok {
				enumSchema = items
			}
		}
		q.Options = mcpEnumOptions(enumSchema)
		if propertyType == "boolean" && len(q.Options) == 0 {
			q.Options = []backend.UserInputOption{{ID: "true", Label: "是"}, {ID: "false", Label: "否"}}
		}
		if len(q.Options) > 0 {
			q.FreeForm = false
			if !q.MultiSelect {
				q.Type = "single_select"
			}
		}
		questions = append(questions, q)
	}
	if len(questions) == 0 {
		questions = []backend.UserInputQuestion{{QuestionID: elicitationChoice, Text: message, Header: "外部工具授權", Type: "single_select", Options: []backend.UserInputOption{{ID: "deny", Label: "拒絕", Recommended: true}, {ID: "approve_once", Label: "允許這次"}}}}
	}
	return questions
}

func mcpEnumOptions(schema map[string]any) []backend.UserInputOption {
	values, _ := schema["enum"].([]any)
	options := make([]backend.UserInputOption, 0, len(values))
	for _, value := range values {
		id := codexAnyString(value)
		options = append(options, backend.UserInputOption{ID: id, Label: id})
	}
	return options
}

func mcpElicitationResponse(raw json.RawMessage, answers map[string]any, cancelled bool) map[string]any {
	if cancelled {
		return mcpDecision("cancel", nil, nil)
	}
	decision := codexAnyString(answers[elicitationChoice])
	if decision == "" {
		decision = codexAnyString(answers["action"])
	}
	switch decision {
	case "deny", "decline":
		return mcpDecision("decline", nil, nil)
	case "approve_once":
		return mcpDecision("accept", nil, map[string]any{"persist": "session"})
	case "approve_always":
		return mcpDecision("accept", nil, map[string]any{"persist": "always"})
	}

	params := decodeObject(raw)
	schema, _ := params["requestedSchema"].(map[string]any)
	properties, _ := schema["properties"].(map[string]any)
	content := map[string]any{}
	for key, value := range answers {
		property, exists := properties[key].(map[string]any)
		if !exists {
			continue
		}
		content[key] = coerceMcpAnswer(value, codexFirstString(property, "type"))
	}
	return mcpDecision("accept", content, nil)
}

func coerceMcpAnswer(value any, schemaType string) any {
	switch schemaType {
	case "boolean":
		if boolean, ok := value.(bool); ok {
			return boolean
		}
		parsed, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(codexAnyString(value))))
		if err == nil {
			return parsed
		}
	case "integer":
		if parsed, err := strconv.Atoi(strings.TrimSpace(codexAnyString(value))); err == nil {
			return parsed
		}
	case "number":
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(codexAnyString(value)), 64); err == nil {
			return parsed
		}
	case "array":
		if _, ok := value.([]any); ok {
			return value
		}
		if value == nil || value == "" {
			return []any{}
		}
		return []any{value}
	}
	return value
}
