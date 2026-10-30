package nativewatch

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"time"
)

// ContextActivity contains only native metadata and a digest, never transcript
// text or compacted summary bytes. Compacted rows are already durable provider
// completions. The initial tail establishes usage only, not a lifetime count.
type ContextActivity struct {
	Session              NativeSession
	TurnID, EventID      string
	Used, Max            *int
	Completed            bool
	Offset, ReportedAtMS int64
}

const contextWindowBytes int64 = 32 << 20

func contextHeader(f *os.File, expected string) bool {
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, 256<<10), 64<<10)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return false
	}
	var row struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	if json.Unmarshal(line, &row) != nil || row.Type != "session_meta" || row.Payload["id"] != expected {
		return false
	}
	return !codexMetaIsSubagent(row.Payload)
}
func contextRowID(kind, thread, turn string, line []byte) string {
	h := sha256.New()
	h.Write([]byte(kind + "\x00" + thread + "\x00"))
	h.Write(line)
	return hex.EncodeToString(h.Sum(nil))
}
func (w *TurnWatcher) pollContext(f *turnFile, size int64, emit func(ContextActivity)) {
	file, err := os.Open(f.session.Path)
	if err != nil {
		return
	}
	defer file.Close()
	if !contextHeader(file, f.session.ResumeID) {
		return
	}
	initial := !f.contextInitialized || size < f.contextOffset
	if initial {
		f.contextInitialized = true
		f.contextOffset = max(0, size-contextWindowBytes)
		f.contextTurn = ""
	}
	start := f.contextOffset
	r := bufio.NewReaderSize(io.NewSectionReader(file, start, min(size-start, contextWindowBytes)), 64<<10)
	offset := start
	skip := f.contextSkipping || initial && start > 0
	f.contextSkipping = false
	var latest *ContextActivity
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if start+contextWindowBytes < size {
				f.contextOffset = offset + int64(len(line))
				f.contextSkipping = true
			}
			break
		} // A partial final row is never committed.
		rowOffset := offset
		offset += int64(len(line))
		f.contextOffset = offset
		if skip {
			skip = false
			continue
		}
		if len(line) > 32<<20 {
			continue
		}
		if !bytes.Contains(line, []byte(`"token_count"`)) && !bytes.Contains(line, []byte(`"compacted"`)) && !bytes.Contains(line, []byte(`"task_started"`)) && !bytes.Contains(line, []byte(`"turn_context"`)) {
			continue
		}
		var row struct {
			Timestamp string `json:"timestamp"`
			Type      string `json:"type"`
			Payload   struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
				Info   *struct {
					Last *struct {
						Total *int `json:"total_tokens"`
					} `json:"last_token_usage"`
					Max *int `json:"model_context_window"`
				} `json:"info"`
			} `json:"payload"`
		}
		if json.Unmarshal(line, &row) != nil {
			continue
		}
		if (row.Type == "turn_context" || row.Type == "event_msg" && row.Payload.Type == "task_started") && row.Payload.TurnID != "" {
			f.contextTurn = row.Payload.TurnID
			continue
		}
		turn := f.contextTurn
		if row.Payload.TurnID != "" {
			turn = row.Payload.TurnID
		}

		timestamp, err := time.Parse(time.RFC3339Nano, row.Timestamp)
		if err != nil || timestamp.UnixMilli() <= 0 || timestamp.After(time.Now().Add(time.Minute)) {
			continue
		}
		a := ContextActivity{Session: f.session, TurnID: turn, Offset: rowOffset, ReportedAtMS: timestamp.UnixMilli()}
		if row.Type == "event_msg" && row.Payload.Type == "token_count" && row.Payload.Info != nil {
			if row.Payload.Info.Last != nil {
				a.Used = row.Payload.Info.Last.Total
			}
			a.Max = row.Payload.Info.Max
			if a.Used != nil && *a.Used < 0 {
				a.Used = nil
			}
			if a.Max != nil && *a.Max <= 0 {
				a.Max = nil
			}
			if a.Used == nil && a.Max == nil {
				continue
			}
			a.EventID = contextRowID("usage", f.session.ResumeID, turn, line)
			if initial {
				latest = &a
			} else {
				emit(a)
			}
		} else if row.Type == "compacted" && !initial {
			a.Completed = true
			a.EventID = contextRowID("compacted", f.session.ResumeID, turn, line)
			emit(a)
		}
	}
	if latest != nil {
		emit(*latest)
	}
}

// HasContextSource verifies the existing provider-owned root header only; it
// does not open/resume a native writer or scan private conversation content.
func HasContextSource(path, thread string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	return err == nil && info.Mode().IsRegular() && contextHeader(f, thread)
}
