package history

import (
	"bufio"
	"bytes"
	"container/list"
	"io"
)

const defaultLoadMaxBytes = 32 << 20

// LoadMaxBytes is the peak-memory budget for parsing one transcript on load.
// A transcript whose retained tail would exceed it is truncated to its most
// recent lines, so a runaway session (observed 800MB+ in a single file) can no
// longer spike the resident bridge to multiple GB when a client opens it.
func LoadMaxBytes() int {
	return envBytes("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", defaultLoadMaxBytes)
}

// TailLine is a retained transcript line tagged with its absolute 1-based line
// number (counting blank/skipped lines). Message ids embed this number, so
// reporting the true line number keeps ids identical whether the file was read
// whole or tail-streamed.
type TailLine struct {
	LineNo int
	Data   []byte
}

// StreamTailLines streams r and returns the trailing non-empty lines whose raw
// bytes fit within budget, each tagged with its absolute line number, plus
// whether earlier lines were dropped. Peak memory is ~budget regardless of file
// size. It reads with ReadBytes (not a fixed-capacity Scanner) so an
// arbitrarily long single line never errors. Line numbering matches the legacy
// `strings.Split(data, "\n")` path: every physical line increments the counter,
// only non-empty lines are returned.
func StreamTailLines(r io.Reader, budget int) ([]TailLine, bool, error) {
	if budget <= 0 {
		budget = defaultLoadMaxBytes
	}
	br := bufio.NewReaderSize(r, 1<<20)
	retained := list.New()
	held := 0
	lineNo := 0
	truncated := false
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
				cp := append([]byte(nil), trimmed...)
				retained.PushBack(TailLine{LineNo: lineNo, Data: cp})
				held += len(cp)
				// Keep at least one line even if it alone exceeds budget.
				for held > budget && retained.Len() > 1 {
					front := retained.Front()
					held -= len(front.Value.(TailLine).Data)
					retained.Remove(front)
					truncated = true
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, truncated, err
		}
	}
	out := make([]TailLine, 0, retained.Len())
	for e := retained.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(TailLine))
	}
	return out, truncated, nil
}
