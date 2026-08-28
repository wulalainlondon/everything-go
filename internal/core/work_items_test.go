package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

func assertNoTypeWithin(t *testing.T, c *Client, typ string, duration time.Duration) {
	t.Helper()
	deadline := time.After(duration)
	for {
		select {
		case data := <-c.send:
			var event map[string]any
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatalf("bad event JSON: %v", err)
			}
			if event["type"] == typ {
				t.Fatalf("unexpected event type %q before client sync request", typ)
			}
		case <-deadline:
			return
		}
	}
}

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

func TestWorkStartRunUsesSessionActorAndProjectsSuccessToReview(t *testing.T) {
	h, exec := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	route(h, c, `{"type":"new_session","session_id":"s1","name":"Run","backend":"codex"}`)
	_ = waitForType(t, c, "session_created")
	route(h, c, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"P"}`)
	_ = waitForType(t, c, "work_mutation_ack")
	route(h, c, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Deliver"}`)
	created := waitForType(t, c, "work_mutation_ack")
	version := uint64(created["entity_version"].(float64))
	route(h, c, `{"type":"work_item_move","work_item_id":"wi1","mutation_id":"mm","expected_version":`+formatUint(version)+`,"lifecycle":"ready"}`)
	moved := waitForType(t, c, "work_mutation_ack")
	version = uint64(moved["entity_version"].(float64))
	route(h, c, `{"type":"work_item_link_session","work_item_id":"wi1","session_id":"s1","mutation_id":"ml","expected_version":`+formatUint(version)+`,"role":"primary"}`)
	linked := waitForType(t, c, "work_mutation_ack")
	version = uint64(linked["entity_version"].(float64))
	exec.onSend = func(s *session.Session, reqID, content string) {
		if content != "implement it" {
			t.Errorf("run content=%q", content)
		}
		h.Emit(protocol.NewDone(s.ID, reqID))
	}
	route(h, c, `{"type":"work_item_start_run","work_item_id":"wi1","session_id":"s1","request_id":"req1","run_id":"run1","run_kind":"implementation","mutation_id":"mr","expected_version":`+formatUint(version)+`,"content":"implement it"}`)
	ack := waitForType(t, c, "work_mutation_ack")
	if ack["run"].(map[string]any)["id"] != "run1" {
		t.Fatalf("run ack=%+v", ack)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		item, err := service.GetItem(context.Background(), "wi1")
		if err == nil && item.Lifecycle == workitems.LifecycleReview {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	item, _ := service.GetItem(context.Background(), "wi1")
	t.Fatalf("explicit successful run did not reach review: %+v", item)
}

func TestWorkRequestOwnershipRemainsDurableAfterTerminalProjection(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	ctx := context.Background()
	project, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p1", Name: "P"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "wi1", ProjectID: project.ID, Title: "Deliver", Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, Lifecycle: workitems.LifecycleReady, ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, item, err = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "link1", WorkItemID: item.ID, SessionID: "s1", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.StartRun(ctx, workitems.StartRunInput{ID: "run1", WorkItemID: item.ID, SessionID: "s1", RequestID: "req1", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.AdvanceRun(ctx, "s1", "req1", "succeeded", ""); err != nil {
		t.Fatal(err)
	}
	owned, err := service.OwnsRequest(ctx, "s1", "req1")
	if err != nil || !owned {
		t.Fatalf("terminal work request ownership: owned=%v err=%v", owned, err)
	}
	owned, err = service.OwnsRequest(ctx, "s1", "ordinary")
	if err != nil || owned {
		t.Fatalf("ordinary request ownership: owned=%v err=%v", owned, err)
	}
	if h.shouldNotifyTaskDone("s1", "req1") {
		t.Fatal("work request would also send legacy task_done notification")
	}
	if !h.shouldNotifyTaskDone("s1", "ordinary") {
		t.Fatal("ordinary request lost legacy task_done notification")
	}
}

func TestWorkHelloAdvertisesCapabilityAndReconcilesFromClientCursor(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"phone"}`)
	hello := waitForType(t, c, "hello_ack")
	capabilities, ok := hello["capabilities"].([]any)
	if !ok || len(capabilities) != 2 || capabilities[0] != "work_coordination_v1" || capabilities[1] != "work_items_v1" {
		t.Fatalf("hello capabilities=%v", hello["capabilities"])
	}
	assertNoTypeWithin(t, c, "work_snapshot", 50*time.Millisecond)
	route(h, c, `{"type":"work_sync_request","revision":0}`)
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
	h2.sendWorkSnapshot(c2, true)
	snapshot := waitForType(t, c2, "work_snapshot")
	items := snapshot["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != "wi1" {
		t.Fatalf("restart snapshot=%+v db=%s", snapshot, filepath.Join(dir, "everything_go_work_items.db"))
	}
}

func TestWorkAttachmentUsesCanonicalJournalAndReportsMissingSource(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	c.clientSurface = "android"
	route(h, c, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"P"}`)
	_ = waitForType(t, c, "work_mutation_ack")
	route(h, c, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Proof"}`)
	created := waitForType(t, c, "work_mutation_ack")
	version := uint64(created["entity_version"].(float64))

	path := filepath.Join(t.TempDir(), "proof.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	record, added := h.attachments.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})
	if !added {
		t.Fatal("canonical attachment was not registered")
	}
	route(h, c, `{"type":"work_item_attachment_add","work_item_id":"wi1","work_attachment_id":"wa1","attachment_id":"`+record.AttachmentID+`","mutation_id":"ma","expected_version":`+formatUint(version)+`}`)
	ack := waitForType(t, c, "work_mutation_ack")
	attachment := ack["attachment"].(map[string]any)
	if attachment["status"] != "available" || attachment["url"] == "" {
		t.Fatalf("materialized attachment=%+v", attachment)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	route(h, c, `{"type":"work_sync_request","revision":0}`)
	snapshot := waitForType(t, c, "work_snapshot")
	attachments := snapshot["attachments"].([]any)
	if len(attachments) != 1 || attachments[0].(map[string]any)["status"] != "missing" {
		t.Fatalf("missing attachment snapshot=%+v", attachments)
	}
	activities := snapshot["activities"].([]any)
	if len(activities) == 0 || activities[0].(map[string]any)["actor"] != "mobile" {
		t.Fatalf("client surface was not preserved in activity: %+v", activities)
	}
}
