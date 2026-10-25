package core

import (
	"os"
	"path/filepath"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Input is internal parsed provider history, never client-supplied paths.
// A missing native turn/request journal cannot be repaired by inventing an ID.
func (h *Hub) recoverHistoryGeneratedImages(s *session.Session, threads []string, messages []map[string]any) {
	if s.Backend() != backend.Codex {
		return
	}
	allowed := map[string]bool{}
	for _, t := range threads {
		allowed[t] = true
	}
	for _, m := range messages {
		if m["source"] != "codex" || m["role"] != "assistant" {
			continue
		}
		thread, _ := m["source_session_id"].(string)
		request, _ := m["request_id"].(string)
		if !allowed[thread] || request == "" {
			continue
		}
		paths, _ := m["generated_image_paths"].([]string)
		if cached, ok := m["generated_image_paths"].([]any); ok {
			for _, value := range cached {
				if path, ok := value.(string); ok {
					paths = append(paths, path)
				}
			}
		}
		for _, p := range paths {
			// Native generators save under the original physical thread. Do not turn a
			// reference to another thread's file into that conversation's attachment.
			if filepath.Clean(p) != p || filepath.Base(filepath.Dir(p)) != thread || filepath.Base(filepath.Dir(filepath.Dir(p))) != "generated_images" || filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(p)))) != ".codex" {
				continue
			}
			info, err := os.Stat(p)
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			h.Emit(protocol.Media{Type: "media", SessionID: s.ID, RequestID: request, MediaType: "image", Path: p})
		}
	}
}
