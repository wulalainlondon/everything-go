package goexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/history"
	"everything-go/internal/session"
)

const recoverySchemaVersion = 1

type checkpointMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type codexCheckpoint struct {
	SchemaVersion  int                 `json:"schema_version"`
	LogicalSession string              `json:"logical_session_id"`
	SourceResumeID string              `json:"source_resume_id"`
	CreatedAt      string              `json:"created_at"`
	Cwd            string              `json:"cwd"`
	Model          string              `json:"model,omitempty"`
	Effort         string              `json:"effort,omitempty"`
	Sandbox        string              `json:"sandbox,omitempty"`
	ServiceTier    string              `json:"service_tier,omitempty"`
	Personality    string              `json:"personality,omitempty"`
	RolloutPath    string              `json:"rollout_path"`
	RolloutBytes   int64               `json:"rollout_bytes"`
	Recent         []checkpointMessage `json:"recent_messages"`
}

type generationRecord struct {
	Generation     int    `json:"generation"`
	ResumeID       string `json:"resume_id"`
	Status         string `json:"status"`
	RolloverReason string `json:"rollover_reason,omitempty"`
	RolloutBytes   int64  `json:"rollout_bytes,omitempty"`
	Checkpoint     string `json:"checkpoint,omitempty"`
	CheckpointSHA  string `json:"checkpoint_sha256,omitempty"`
	CommittedAt    string `json:"committed_at,omitempty"`
}

type generationManifest struct {
	SchemaVersion    int                `json:"schema_version"`
	LogicalSessionID string             `json:"logical_session_id"`
	ActiveGeneration int                `json:"active_generation"`
	ActiveResumeID   string             `json:"active_resume_id"`
	Generations      []generationRecord `json:"generations"`
	Pending          *generationRecord  `json:"pending_transaction,omitempty"`
}

type codexRecovery struct {
	ManifestPath  string
	CheckpointRel string
	CheckpointSHA string
	OldResumeID   string
	RolloutBytes  int64
	Reason        string
	Generation    int
	Handoff       string
}

var recoverySecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{12,}`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{12,}`),
	regexp.MustCompile(`(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|password|secret)\s*[:=]\s*[^\s,;]+`),
}

func envBool(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envInt64(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func fileSize(path string) int64 {
	if path == "" {
		return 0
	}
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return 0
	}
	return fi.Size()
}

func redactRecoveryText(text string) string {
	for _, pattern := range recoverySecretPatterns {
		text = pattern.ReplaceAllString(text, "$1[REDACTED]")
	}
	return text
}

func checkpointContent(message map[string]any) string {
	content, _ := message["content"].(string)
	content = redactRecoveryText(strings.TrimSpace(content))
	if len(content) > 8*1024 {
		content = content[:8*1024] + "…"
	}
	return content
}

func (c *Codex) prepareColdRecovery(snap session.Snapshot, rolloutPath string, rolloutBytes int64, reason string) (*codexRecovery, error) {
	if snap.ResumeID == "" || rolloutPath == "" || rolloutBytes <= 0 {
		return nil, errors.New("source rollout is unavailable")
	}
	res, err := c.LoadHistory(snap.ResumeID, history.Opts{Limit: 40, Mode: "snapshot"})
	if err != nil {
		return nil, err
	}
	recent := make([]checkpointMessage, 0, len(res.Messages))
	for _, message := range res.Messages {
		role, _ := message["role"].(string)
		content := checkpointContent(message)
		if (role == "user" || role == "assistant") && content != "" {
			recent = append(recent, checkpointMessage{Role: role, Content: content})
		}
	}
	if len(recent) == 0 {
		return nil, errors.New("history tail produced no usable checkpoint messages")
	}

	checkpoint := codexCheckpoint{
		SchemaVersion: recoverySchemaVersion, LogicalSession: snap.ID,
		SourceResumeID: snap.ResumeID, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Cwd: snap.Cwd, Model: snap.Model, Effort: snap.Effort, Sandbox: snap.Sandbox,
		ServiceTier: snap.ServiceTier, Personality: snap.Personality,
		RolloutPath: rolloutPath, RolloutBytes: rolloutBytes, Recent: recent,
	}
	maxBytes := c.checkpointMaxBytes
	if maxBytes < 4*1024 {
		maxBytes = 4 * 1024
	}
	var checkpointJSON []byte
	for len(checkpoint.Recent) > 0 {
		checkpointJSON, err = json.MarshalIndent(checkpoint, "", "  ")
		if err != nil {
			return nil, err
		}
		if len(checkpointJSON) <= maxBytes {
			break
		}
		checkpoint.Recent = checkpoint.Recent[1:]
	}
	if len(checkpoint.Recent) == 0 || len(checkpointJSON) > maxBytes {
		return nil, fmt.Errorf("checkpoint exceeds %d-byte limit", maxBytes)
	}

	dir := filepath.Join(c.dataDir, "codex-generations", safeGenerationID(snap.ID))
	manifestPath := filepath.Join(dir, "manifest.json")
	manifest := loadGenerationManifest(manifestPath, snap)
	generation := manifest.ActiveGeneration + 1
	if generation < 2 {
		generation = 2
	}
	checkpointRel := fmt.Sprintf("checkpoint-%06d.json", generation)
	checkpointPath := filepath.Join(dir, checkpointRel)
	sum := sha256.Sum256(checkpointJSON)
	checkpointSHA := hex.EncodeToString(sum[:])
	pending := generationRecord{
		Generation: generation, Status: "prepared", RolloverReason: reason,
		RolloutBytes: rolloutBytes, Checkpoint: checkpointRel, CheckpointSHA: checkpointSHA,
	}
	manifest.Pending = &pending
	if err := writeAtomic(checkpointPath, checkpointJSON, 0o600); err != nil {
		return nil, err
	}
	if err := writeAtomicJSON(manifestPath, manifest, 0o600); err != nil {
		return nil, err
	}
	handoff := "<bridge_session_handoff schema=\"1\">\n" + string(checkpointJSON) + "\n</bridge_session_handoff>"
	return &codexRecovery{
		ManifestPath: manifestPath, CheckpointRel: checkpointRel, CheckpointSHA: checkpointSHA,
		OldResumeID: snap.ResumeID, RolloutBytes: rolloutBytes, Reason: reason,
		Generation: generation, Handoff: handoff,
	}, nil
}

func (c *Codex) commitColdRecovery(recovery *codexRecovery, newResumeID string) error {
	if recovery == nil || newResumeID == "" {
		return errors.New("missing recovery transaction or new resume id")
	}
	manifest := generationManifest{}
	data, err := os.ReadFile(recovery.ManifestPath)
	if err != nil || json.Unmarshal(data, &manifest) != nil {
		return errors.New("prepared generation manifest is unreadable")
	}
	if manifest.Pending == nil || manifest.Pending.Generation != recovery.Generation || manifest.Pending.CheckpointSHA != recovery.CheckpointSHA {
		return errors.New("prepared generation transaction does not match")
	}
	if len(manifest.Generations) == 0 {
		manifest.Generations = append(manifest.Generations, generationRecord{
			Generation: 1, ResumeID: recovery.OldResumeID, Status: "archived",
			RolloverReason: recovery.Reason, RolloutBytes: recovery.RolloutBytes,
		})
	} else {
		for i := range manifest.Generations {
			if manifest.Generations[i].ResumeID == recovery.OldResumeID {
				manifest.Generations[i].Status = "archived"
			}
		}
	}
	committed := *manifest.Pending
	committed.ResumeID = newResumeID
	committed.Status = "active"
	committed.CommittedAt = time.Now().UTC().Format(time.RFC3339Nano)
	manifest.Generations = append(manifest.Generations, committed)
	manifest.ActiveGeneration = committed.Generation
	manifest.ActiveResumeID = newResumeID
	manifest.Pending = nil
	return writeAtomicJSON(recovery.ManifestPath, manifest, 0o600)
}

func loadGenerationManifest(path string, snap session.Snapshot) generationManifest {
	var manifest generationManifest
	if data, err := os.ReadFile(path); err == nil && json.Unmarshal(data, &manifest) == nil && manifest.SchemaVersion == recoverySchemaVersion {
		return manifest
	}
	return generationManifest{
		SchemaVersion: recoverySchemaVersion, LogicalSessionID: snap.ID,
		ActiveGeneration: 1, ActiveResumeID: snap.ResumeID,
		Generations: []generationRecord{{Generation: 1, ResumeID: snap.ResumeID, Status: "active"}},
	}
}

func safeGenerationID(id string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, id)
}

func writeAtomicJSON(path string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, data, mode)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
