package goexec

import (
	"everything-go/internal/lifecycle"
	"log"
	"os/exec"
)

func (c *Codex) recordLifecycle(event lifecycle.Event) {
	if err := c.lifecycle.Add(event); err != nil {
		log.Printf("[codex] lifecycle evidence unavailable: %v", err)
	}
}

// A known local kill attempt is distinct from the later kernel-observed exit.
func (c *Codex) killOwnedProcess(cmd *exec.Cmd, identity lifecycle.ProcessIdentity, epoch uint64, reason string) {
	c.recordLifecycle(lifecycle.Event{Kind: "process_kill_attempt", Owner: "bridge_stdio_child", PID: cmd.Process.Pid, Process: &identity, Epoch: epoch, Reason: reason})
	result := "kill_request_returned_success_not_exit_proof"
	if cmd.Process.Kill() != nil {
		result = "kill_request_returned_error_exit_unknown"
	}
	c.recordLifecycle(lifecycle.Event{Kind: "process_kill_result", Owner: "bridge_stdio_child", PID: cmd.Process.Pid, Process: &identity, Epoch: epoch, Reason: result})
}
