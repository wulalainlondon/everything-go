package core

import (
	"context"
	"path/filepath"
	"testing"

	"everything-go/internal/workitems"
)

func attachWorkService(t *testing.T, h *Hub, dir string) *workitems.Service {
	t.Helper()
	service, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	h.SetWorkItems(service)
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func TestWorkHelloAdvertisesCapabilityAndSnapshot(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"phone"}`)
	hello := waitForType(t, c, "hello_ack")
	capabilities, ok := hello["capabilities"].([]any)
	if !ok || len(capabilities) != 1 || capabilities[0] != "work_items_v1" {
		t.Fatalf("hello capabilities=%v", hello["capabilities"])
	}
	snapshot := waitForType(t, c, "work_snapshot")
	if _, ok := snapshot["projects"].([]any); !ok {
		t.Fatalf("work snapshot collections must be arrays: %+v", snapshot)
	}
}

func TestWorkMutationBroadcastAndPerDeviceRead(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	a := newTestClient(h)
	a.deviceID = "device-a"
	b := newTestClient(h)
	b.deviceID = "device-b"

	route(h, a, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"Bridge"}`)
	_ = waitForType(t, a, "work_mutation_ack")
	_ = waitForType(t, a, "work_delta_batch")
	_ = waitForType(t, b, "work_delta_batch")

	route(h, a, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Native Work"}`)
	ack := waitForType(t, a, "work_mutation_ack")
	item := ack["item"].(map[string]any)
	revision := uint64(item["activity_revision"].(float64))
	_ = waitForType(t, a, "work_delta_batch")
	_ = waitForType(t, b, "work_delta_batch")

	// Lost-ACK retry returns the durable original response without a second
	// entity mutation or another broadcast.
	route(h, a, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Native Work"}`)
	retry := waitForType(t, a, "work_mutation_ack")
	if retry["revision"] != ack["revision"] {
		t.Fatalf("retry response changed: first=%+v retry=%+v", ack, retry)
	}

	route(h, a, `{"type":"work_item_read","work_item_id":"wi1","revision":`+formatUint(revision)+`}`)
	read := waitForType(t, a, "work_read_ack")
	if read["item"].(map[string]any)["unread"] != float64(0) {
		t.Fatalf("read ACK=%+v", read)
	}
	as, err := service.SnapshotForDevice(context.Background(), "device-a")
	if err != nil {
		t.Fatal(err)
	}
	bs, err := service.SnapshotForDevice(context.Background(), "device-b")
	if err != nil {
		t.Fatal(err)
	}
	if as.Items[0].Unread != 0 || bs.Items[0].Unread != 1 {
		t.Fatalf("device read leaked: a=%d b=%d", as.Items[0].Unread, bs.Items[0].Unread)
	}
}

func TestWorkSnapshotSurvivesBridgeRestart(t *testing.T) {
	dir := t.TempDir()
	h1, _ := newTestHub(t)
	service1, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	h1.SetWorkItems(service1)
	c1 := newTestClient(h1)
	c1.deviceID = "desktop"
	route(h1, c1, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"Persist"}`)
	_ = waitForType(t, c1, "work_mutation_ack")
	route(h1, c1, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Restart proof"}`)
	_ = waitForType(t, c1, "work_mutation_ack")
	if err := service1.Close(); err != nil {
		t.Fatal(err)
	}

	h2, _ := newTestHub(t)
	service2, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer service2.Close()
	h2.SetWorkItems(service2)
	c2 := newTestClient(h2)
	c2.deviceID = "phone"
	h2.sendWorkSnapshot(c2)
	snapshot := waitForType(t, c2, "work_snapshot")
	items := snapshot["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != "wi1" {
		t.Fatalf("restart snapshot=%+v db=%s", snapshot, filepath.Join(dir, "everything_go_work_items.db"))
	}
}
