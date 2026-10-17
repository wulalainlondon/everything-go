package history

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Compressed archives are streamed rather than expanded to an ordinary file.
// Retained JSON lines and look-ahead stay bounded; absolute source IDs survive.
func ReadStreamWindow(reader io.Reader, cursorLine, budget int) (FileWindow, error) {
	if budget <= 0 {
		budget = defaultLoadMaxBytes
	}
	br := bufio.NewReaderSize(reader, 1<<20)
	var lines []TailLine
	held, head, lineNo, after := 0, 0, 0, 0
	moreAfter := false
	var context *TailLine
	for {
		var raw []byte
		var err error
		for {
			var part []byte
			part, err = br.ReadSlice('\n')
			if len(raw)+len(part) > 2*budget {
				return FileWindow{}, fmt.Errorf("history row exceeds bounded archive page budget")
			}
			raw = append(raw, part...)
			if err != bufio.ErrBufferFull {
				break
			}
		}
		if len(raw) > 0 {
			lineNo++
			if cursorLine > 0 && lineNo > cursorLine {
				after += len(raw)
			}
			if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 {
				lines = append(lines, TailLine{LineNo: lineNo, Data: trimmed})
				held += len(trimmed)
			}
			for held > 2*budget && len(lines)-head > 1 {
				old := lines[head]
				if marker, ok := streamControlMarker(old); ok {
					copy := marker
					context = &copy
				}
				held -= len(old.Data)
				lines[head] = TailLine{}
				head++
			}
			if head > 1024 {
				lines = append([]TailLine(nil), lines[head:]...)
				head = 0
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return FileWindow{}, err
		}
		if cursorLine > 0 && lineNo >= cursorLine && after >= budget/4 {
			_, e := br.Peek(1)
			moreAfter = e == nil
			if e != nil && e != io.EOF {
				return FileWindow{}, e
			}
			break
		}
	}
	if cursorLine > lineNo {
		return FileWindow{}, fmt.Errorf("history archive cursor line missing")
	}
	lines = lines[head:]
	moreBefore := len(lines) > 0 && lines[0].LineNo > 1
	startLine := 1
	if len(lines) > 0 {
		startLine = lines[0].LineNo
	}
	if context != nil && moreBefore {
		lines = append([]TailLine{*context}, lines...)
	}
	return FileWindow{Lines: lines, MoreBefore: moreBefore, MoreAfter: moreAfter, StartLine: startLine, EndLine: lineNo}, nil
}
func streamControlMarker(line TailLine) (TailLine, bool) {
	if len(line.Data) > 65536 || !bytes.Contains(line.Data, []byte("turn")) {
		return TailLine{}, false
	}
	var row struct {
		Type    string `json:"type"`
		Payload struct {
			Type   string          `json:"type"`
			TurnID string          `json:"turn_id"`
			Error  json.RawMessage `json:"error"`
		} `json:"payload"`
	}
	if json.Unmarshal(line.Data, &row) != nil {
		return TailLine{}, false
	}
	if row.Type != "turn_context" && (row.Type != "event_msg" || (row.Payload.Type != "task_started" && row.Payload.Type != "task_complete" && row.Payload.Type != "turn_aborted")) {
		return TailLine{}, false
	}
	data, _ := json.Marshal(row)
	return TailLine{LineNo: line.LineNo, Data: data}, true
}
