package core

import (
	"path/filepath"
	"testing"
)

func TestInstanceUpsertListDeleteProtocol(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	root := t.TempDir()
	data := filepath.Join(t.TempDir(), "child")
	route(h, c, `{"type":"upsert_instance","name":"studio","port":9453,"root_dir":"`+root+`","data_dir":"`+data+`","backend":"codex"}`)
	upserted := waitForType(t, c, "instance_upserted")
	if upserted["ok"] != true || upserted["code"] != nil {
		t.Fatalf("upsert = %+v", upserted)
	}

	route(h, c, `{"type":"list_instances"}`)
	listed := waitForType(t, c, "instances_list")
	items := listed["instances"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["state"] != "stopped" {
		t.Fatalf("list = %+v", listed)
	}

	route(h, c, `{"type":"delete_instance","name":"studio"}`)
	deleted := waitForType(t, c, "instance_deleted")
	if deleted["ok"] != true {
		t.Fatalf("delete = %+v", deleted)
	}
}

func TestInstanceCommandsAreMasterOnly(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.RootDir = t.TempDir()
	c := newTestClient(h)
	route(h, c, `{"type":"list_instances"}`)
	event := waitForType(t, c, "instance_error")
	if event["code"] != "not_master" {
		t.Fatalf("event = %+v", event)
	}
}
