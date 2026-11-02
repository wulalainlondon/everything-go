package lifecycle

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ObserveProcess is one bounded metadata-only snapshot. An absent or timed-out
// observation remains explicitly unknown; it cannot invent a process exit.
func ObserveProcess(pid int) ProcessIdentity {
	result := ProcessIdentity{PID: pid, Observation: "identity_unavailable"}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/ps", "-ww", "-p", strconv.Itoa(pid), "-o", "ppid=,pgid=,lstart=,comm=")
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	raw, err := cmd.Output()
	if err != nil || len(raw) > 8192 {
		return result
	}
	p := strings.Fields(string(raw))
	if len(p) < 8 {
		return result
	}
	parent, e1 := strconv.Atoi(p[0])
	group, e2 := strconv.Atoi(p[1])
	if e1 != nil || e2 != nil {
		return result
	}
	result.ParentPID = parent
	result.Group = group
	result.Start = strings.Join(p[2:7], " ")
	result.Executable = strings.Join(p[7:], " ")
	result.Observation = "os_metadata_observed"
	return result
}
