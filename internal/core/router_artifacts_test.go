package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanArtifactsUsesCurrentMediaURLAndJail(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "render.png")
	if err := os.WriteFile(file, []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, _ := newTestHub(t)
	h.cfg.RootDir = root
	h.SetTunnelURL("https://fresh.example")
	c := newTestClient(h)

	route(h, c, `{"type":"scan_artifacts","path":"`+root+`","limit":10}`)
	event := waitForType(t, c, "artifacts_list")
	items := event["artifacts"].([]any)
	if len(items) != 1 {
		t.Fatalf("artifacts = %+v", items)
	}
	artifact := items[0].(map[string]any)
	realFile, err := filepath.EvalSymlinks(file)
	if err != nil {
		t.Fatal(err)
	}
	if artifact["url"] != "https://fresh.example/media"+realFile {
		t.Fatalf("url = %v", artifact["url"])
	}

	route(h, c, `{"type":"scan_artifacts","path":"/tmp","limit":10}`)
	event = waitForType(t, c, "artifacts_list")
	if len(event["artifacts"].([]any)) != 0 {
		t.Fatalf("jail escape returned artifacts: %+v", event)
	}
}

func TestYouTubeTaskEmptyURLHasStartedThenFailedLifecycle(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"youtube_task","session_id":"s1","url":""}`)
	started := waitForType(t, c, "youtube_task_started")
	failed := waitForType(t, c, "youtube_task_failed")
	if started["task_id"] != failed["task_id"] {
		t.Fatalf("task IDs differ: %v / %v", started, failed)
	}
	if failed["message"] != "YouTube URL is required" {
		t.Fatalf("message = %v", failed["message"])
	}
	artifact := failed["artifact"].(map[string]any)
	if artifact["status"] != "failed" || artifact["source_session_id"] != "s1" {
		t.Fatalf("artifact = %+v", artifact)
	}
}
