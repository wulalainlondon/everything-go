package agentactivity

import (
	"bufio"
	"encoding/json"
	"io"
)

const MaxNativePrefixRecord = 1 << 20

// A tail can be oversized and partial; Records still counts it exactly once.
// Only byte counts survive, never omitted content.
type NativePrefixStats struct {
	Records, Unrelated, Malformed, RejectedNative, NativeEvents uint64
	Oversized, PartialTail, OversizedBytes, PartialTailBytes    uint64
}

// ReadNativePrefix qualifies an explicitly captured original prefix. It does
// not open/discover files, authenticate callers, install a reader or relax any
// existing production file guard. Its caller owns those boundaries.
func ReadNativePrefix(reader io.Reader, root string, accept func([]byte) error) (NativePrefixStats, error) {
	stats := NativePrefixStats{}
	if !shortID(root) || accept == nil {
		return stats, ErrNativeSource
	}
	r := bufio.NewReaderSize(reader, 65536)
	header, _, terminated, oversized, err := nativePrefixLine(r)
	if err != nil || !terminated || oversized || !uniqueJSON(header) {
		return stats, ErrNativeSource
	}
	var meta struct {
		Type    string
		Payload struct{ ID string }
	}
	if json.Unmarshal(header, &meta) != nil || meta.Type != "session_meta" || meta.Payload.ID != root {
		return stats, ErrNativeSource
	}
	for {
		raw, n, complete, over, e := nativePrefixLine(r)
		if e == io.EOF && n == 0 {
			return stats, nil
		}
		if e != nil && e != io.EOF {
			return stats, e
		}
		stats.Records++
		if over {
			stats.Oversized++
			stats.OversizedBytes += uint64(n)
		}
		if !complete {
			stats.PartialTail++
			stats.PartialTailBytes += uint64(n)
		}
		if over || !complete {
			if e == io.EOF {
				return stats, nil
			}
			continue
		}
		if !uniqueJSON(raw) {
			stats.Malformed++
			continue
		}
		var row struct {
			Type    string
			Payload struct {
				Type string
				Item struct{ Type string }
			}
		}
		if json.Unmarshal(raw, &row) != nil {
			stats.Malformed++
			continue
		}
		if row.Type != "event_msg" || row.Payload.Type != "item_completed" || row.Payload.Item.Type != "SubAgentActivity" {
			stats.Unrelated++
			continue
		}
		b, e := DecodeRolloutActivity(raw, root)
		if e != nil {
			stats.RejectedNative++
			continue
		}
		if e = accept(b); e != nil {
			return stats, e
		}
		stats.NativeEvents++
	}
}

func nativePrefixLine(r *bufio.Reader) (raw []byte, n int64, complete, oversized bool, err error) {
	for {
		part, e := r.ReadSlice('\n')
		n += int64(len(part))
		if n > MaxNativePrefixRecord {
			oversized = true
			raw = nil
		}
		if !oversized {
			raw = append(raw, part...)
		}
		if e == bufio.ErrBufferFull {
			continue
		}
		if e == nil {
			return raw, n, true, oversized, nil
		}
		return raw, n, false, oversized, e
	}
}
