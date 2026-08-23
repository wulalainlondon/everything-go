package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/instances"
)

func (h *Hub) handleInstanceCommand(c *Client, cmd clientproto.Command) {
	if h.cfg.RootDir != "" {
		c.enqueueEvent(h.client.InstanceError(cmd.Kind, cmd.Name, "not_master", "Instance management is only available on the master bridge"))
		return
	}
	switch cmd.Kind {
	case "list_instances":
		c.enqueueEvent(h.client.InstancesList(h.instances.List()))
	case "upsert_instance":
		dataDir := strings.TrimSpace(cmd.DataDir)
		if dataDir == "" {
			home, _ := os.UserHomeDir()
			dataDir = filepath.Join(home, ".bridge-instances", cmd.Name)
		}
		status, code := h.instances.Upsert(instances.Instance{Name: cmd.Name, Port: cmd.Port, RootDir: cmd.RootDir, DataDir: dataDir, Backend: cmd.Backend, Model: cmd.Model})
		if code != "" {
			c.enqueueEvent(h.client.InstanceUpserted(cmd.Name, nil, code))
			return
		}
		c.enqueueEvent(h.client.InstanceUpserted(cmd.Name, &status, ""))
		h.Emit(h.client.InstancesList(h.instances.List()))
	case "start_instance":
		code := h.instances.Start(cmd.Name)
		c.enqueueEvent(h.client.InstanceAction("instance_started", cmd.Name, code))
		h.Emit(h.client.InstancesList(h.instances.List()))
	case "stop_instance":
		code := h.instances.Stop(cmd.Name)
		if code == "default_immutable" {
			c.enqueueEvent(h.client.InstanceError(cmd.Kind, cmd.Name, code, "The default instance cannot be stopped"))
			return
		}
		c.enqueueEvent(h.client.InstanceAction("instance_stopped", cmd.Name, code))
		h.Emit(h.client.InstancesList(h.instances.List()))
	case "delete_instance":
		code := h.instances.Delete(cmd.Name)
		if code == "default_immutable" {
			c.enqueueEvent(h.client.InstanceError(cmd.Kind, cmd.Name, code, "The default instance cannot be deleted"))
			return
		}
		c.enqueueEvent(h.client.InstanceAction("instance_deleted", cmd.Name, code))
		h.Emit(h.client.InstancesList(h.instances.List()))
	default:
		c.enqueueEvent(h.client.InstanceError(cmd.Kind, cmd.Name, "unsupported", fmt.Sprintf("Unsupported instance command %q", cmd.Kind)))
	}
}
