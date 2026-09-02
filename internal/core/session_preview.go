package core

import (
	"regexp"
	"strings"

	"everything-go/internal/protocol"
	"github.com/rivo/uniseg"
)

const sessionPreviewPolicyVersion = 1

var previewWhitespace = regexp.MustCompile(`\s+`)

// selectSessionPreview is the sole row-preview selection policy. Active work
// describes the current user request, waiting work shows the assistant's
// question, and terminal/idle work shows the latest assistant result with a
// user fallback. Clients render this projection instead of reselecting a
// different message from their local cache.
func selectSessionPreview(messages []protocol.RecentMessage, phase string) (text, role string) {
	want := "assistant"
	if phase == "queued" || phase == "running" || phase == "stopping" {
		want = "user"
	}
	if value := lastPreviewByRole(messages, want); value != "" {
		return value, want
	}
	fallback := "user"
	if want == "user" {
		fallback = "assistant"
	}
	return lastPreviewByRole(messages, fallback), fallback
}

func lastPreviewByRole(messages []protocol.RecentMessage, role string) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != role {
			continue
		}
		text := normalizePreviewText(messages[i].Text)
		if text != "" {
			return truncateGraphemes(text, 160)
		}
	}
	return ""
}

func normalizePreviewText(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(previewWhitespace.ReplaceAllString(value, " "))
}

func truncateGraphemes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	g := uniseg.NewGraphemes(value)
	var b strings.Builder
	for count := 0; count < limit && g.Next(); count++ {
		b.WriteString(g.Str())
	}
	return b.String()
}
