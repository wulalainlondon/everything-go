package goexec

import (
	"everything-go/internal/lifecycle"
	"log"
)

func (c *Codex) recordLifecycle(event lifecycle.Event) {
	event.Epoch = c.rpc.transportEpoch.Load()
	if err := c.lifecycle.Add(event); err != nil {
		log.Printf("[codex] lifecycle evidence unavailable: %v", err)
	}
}
