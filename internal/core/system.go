package core

import (
	"time"

	"everything-go/internal/protocol"
)

// sessionsBatchSize matches Python's send_all_sessions batch_size.
const sessionsBatchSize = 50

// handleGetAllSessions streams the full visible session list to one client in
// batches of 50, each carrying offset/total/done so the app can assemble the
// snapshot incrementally. Mirrors session_registry.send_all_sessions.
//
// Run in its own goroutine: the inter-batch pause must not block the reader.
// Parity note: an empty list emits nothing (Python's range loop is empty too).
func (h *Hub) handleGetAllSessions(c *Client) {
	all := h.sessionSummaries()
	total := len(all)
	for offset := 0; offset < total; offset += sessionsBatchSize {
		end := offset + sessionsBatchSize
		if end > total {
			end = total
		}
		done := offset+sessionsBatchSize >= total
		c.enqueueEvent(protocol.NewSessionsListAppend(all[offset:end], offset, total, done))
		if !done {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// handleRestart triggers a bridge restart. Mirrors system_ops.restart_bridge:
// if no restart action is configured we answer with an error, otherwise we ack
// and fire it. Python touches a trigger file watched by an external launchd
// agent; Go (no such agent on the experiment port) self-re-execs instead. The
// action runs async so the ack flushes before the process image is replaced.
func (h *Hub) handleRestart(c *Client) {
	if h.restart == nil {
		c.enqueueEvent(protocol.NewError("", "", "Restart not configured on this bridge"))
		return
	}
	c.enqueueEvent(protocol.NewRestartAck())
	go h.restart()
}
