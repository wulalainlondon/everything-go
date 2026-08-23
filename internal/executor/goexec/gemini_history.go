package goexec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"everything-go/internal/history"
)

type geminiChat struct {
	SessionID string          `json:"sessionId"`
	ID        string          `json:"id"`
	Cwd       string          `json:"cwd"`
	Messages  []geminiMessage `json:"messages"`
	Turns     []geminiMessage `json:"turns"`
}

type geminiMessage struct {
	Role      string          `json:"role"`
	Type      string          `json:"type"`
	Author    string          `json:"author"`
	Parts     json.RawMessage `json:"parts"`
	Content   json.RawMessage `json:"content"`
	Timestamp any             `json:"timestamp"`
	CreatedAt any             `json:"created_at"`
}

func geminiChatFiles() []string {
	home := strings.TrimSpace(os.Getenv("GEMINI_HOME"))
	if home == "" {
		userHome, _ := os.UserHomeDir()
		home = filepath.Join(userHome, ".gemini")
	}
	files, _ := filepath.Glob(filepath.Join(home, "tmp", "*", "chats", "*.json"))
	return files
}

func (g *Gemini) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	var result []history.ResumableSession
	for _, name := range geminiChatFiles() {
		chat, info, ok := loadGeminiChat(name)
		if !ok {
			continue
		}
		sid := firstGeminiNonEmpty(chat.SessionID, chat.ID, strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)))
		messages := chat.Messages
		if len(messages) == 0 {
			messages = chat.Turns
		}
		title := sid
		if len(title) > 8 {
			title = title[:8]
		}
		for _, message := range messages {
			if roleOf(message) == "user" {
				if text := geminiMessageText(message); text != "" {
					title = text
					if len(title) > 80 {
						title = title[:80]
					}
					break
				}
			}
		}
		cwd := chat.Cwd
		if cwd == "" {
			cwd, _ = os.UserHomeDir()
		}
		result = append(result, history.ResumableSession{ID: "gemini_" + prefix(sid, 12), Name: title, ClaudeUUID: sid, LastUsed: info.ModTime().UnixMilli(), Cwd: cwd, Backend: "gemini"})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LastUsed > result[j].LastUsed })
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (g *Gemini) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	for _, name := range geminiChatFiles() {
		chat, info, ok := loadGeminiChat(name)
		if !ok {
			continue
		}
		sid := firstGeminiNonEmpty(chat.SessionID, chat.ID, strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)))
		stem := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
		if sid != resumeID && !strings.HasPrefix(sid, resumeID) && !strings.HasPrefix(resumeID, sid) && !strings.HasPrefix(stem, resumeID) {
			continue
		}
		rows := chat.Messages
		if len(rows) == 0 {
			rows = chat.Turns
		}
		messages := make([]map[string]any, 0, len(rows))
		for index, row := range rows {
			role := roleOf(row)
			if role != "user" && role != "assistant" {
				continue
			}
			text := geminiMessageText(row)
			if text == "" {
				continue
			}
			timestamp := geminiTimestamp(firstAny(row.Timestamp, row.CreatedAt))
			if timestamp == 0 {
				timestamp = info.ModTime().UnixMilli()
			}
			messages = append(messages, history.CompleteMsg("gemini", resumeID, "gemini:"+resumeID+":msg:"+itoa(index), role, text, timestamp, []map[string]any{{"type": "text", "text": text}}))
		}
		return history.Slice(messages, opts), nil
	}
	return history.Slice(nil, opts), nil
}

func loadGeminiChat(name string) (geminiChat, os.FileInfo, bool) {
	data, err := os.ReadFile(name)
	if err != nil {
		return geminiChat{}, nil, false
	}
	info, err := os.Stat(name)
	if err != nil {
		return geminiChat{}, nil, false
	}
	var chat geminiChat
	if json.Unmarshal(data, &chat) != nil {
		return geminiChat{}, nil, false
	}
	return chat, info, true
}

func roleOf(message geminiMessage) string {
	role := firstGeminiNonEmpty(message.Role, message.Type, message.Author)
	if role == "model" || role == "gemini" {
		return "assistant"
	}
	return role
}

func geminiMessageText(message geminiMessage) string {
	raw := message.Parts
	if len(raw) == 0 {
		raw = message.Content
	}
	var plain string
	if json.Unmarshal(raw, &plain) == nil {
		return plain
	}
	var parts []any
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var texts []string
	for _, part := range parts {
		if object, ok := part.(map[string]any); ok {
			if text, ok := object["text"].(string); ok && text != "" {
				texts = append(texts, text)
			}
		}
	}
	return strings.TrimSpace(strings.Join(texts, "\n"))
}

func geminiTimestamp(value any) int64 {
	switch value := value.(type) {
	case float64:
		if value > 1e12 {
			return int64(value)
		}
		return int64(value * 1000)
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func firstGeminiNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
func firstAny(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
func prefix(value string, length int) string {
	if len(value) > length {
		return value[:length]
	}
	return value
}
