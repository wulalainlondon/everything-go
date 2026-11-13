package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func audioRuntimeVersion(version string) bool {
	parts := strings.Split(strings.TrimPrefix(version, "codex-cli "), ".")
	if len(parts) < 2 {
		return false
	}
	major, e1 := strconv.Atoi(parts[0])
	minor, e2 := strconv.Atoi(parts[1])
	return e1 == nil && e2 == nil && (major > 0 || minor >= 168)
}

func (c *Codex) SupportsAudioInput(ctx context.Context, s *session.Session) bool {
	c.catalogMu.RLock()
	def := c.catalog
	c.catalogMu.RUnlock()
	c.runtimeMu.Lock()
	diag, _ := c.runtimeDiagnostics["codex"].(map[string]any)
	version, _ := diag["running_version"].(string)
	c.runtimeMu.Unlock()
	if !audioRuntimeVersion(version) {
		return false
	}
	model := s.Snapshot().Model
	if model == "" {
		return false
	} // Never infer an imported native thread's model.
	for _, m := range def.Models {
		if m.ID != model {
			continue
		}
		for _, modality := range m.InputModalities {
			if modality == "audio" {
				return true
			}
		}
	}
	return false
}

func (c *Codex) validateAudioInput(ctx context.Context, s *session.Session, files []backend.FileAttachment) error {
	for _, f := range files {
		if backend.IsAudioType(f.MediaType) && !c.SupportsAudioInput(ctx, s) {
			return errors.New("目前模型或 Codex 執行端不支援錄音附件；原附件保留，未送出音訊，也未自動轉錄")
		}
	}
	return nil
}

func (c *Codex) prepareCodexAudioInput(s *session.Session, reqID, owner string, f backend.FileAttachment, st *codexState) (map[string]any, error) {
	ext := backend.AudioExtension(f.MediaType)
	if ext == "" || f.VerifiedAudioPath == "" || f.VerifiedAudioPath != f.RemotePath || f.SizeBytes <= 0 || f.SizeBytes > backend.MediaAudioMaxBytes || len(f.SHA256) != 64 || f.Content != "" {
		return nil, errors.New("unverified audio attachment")
	}
	root := filepath.Join(c.dataDir, "codex-input-audio")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(root, "recording-*"+ext)
	if err != nil {
		return nil, err
	}
	path := temp.Name()
	err = backend.CopyVerifiedImage(temp, backend.ImageAttachment{VerifiedPath: f.VerifiedAudioPath, SizeBytes: f.SizeBytes, SHA256: f.SHA256})
	closeErr := temp.Close()
	if err != nil || closeErr != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("audio bytes unavailable: %v / %v", err, closeErr)
	}
	st.mu.Lock()
	if st.tempImages == nil {
		st.tempImages = map[string][]string{}
	}
	st.tempImages[owner] = append(st.tempImages[owner], path)
	st.mu.Unlock()
	return map[string]any{"type": "localAudio", "path": path}, nil
}

// Preserve audio-only user turns without decoding or fetching their payload.
func codexHistoryHasAudio(raw []byte) bool {
	var items []struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &items) != nil {
		return false
	}
	for _, item := range items {
		if item.Type == "input_audio" || item.Type == "audio" || item.Type == "localAudio" {
			return true
		}
	}
	return false
}
