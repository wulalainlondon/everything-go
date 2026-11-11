package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Exercise the real decoder/router and both original sends. No service/network
// is started; a temp jail also proves that response.path is not a request ID.
func TestDirectoryCorrelationPreservesCanonicalFallbackAndLegacyStages(t *testing.T) {
	h, c, root, outside := rootScopeFixture(t)
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, actual, id string }{
		{"~", root, "home-original"},
		{link, target, "symlink-original"},
		{outside, root, "fallback-original"},
		{root, root, "same-path-A"},
		{root, root, "same-path-B"},
		{root, root, ""},
	} {
		wire, err := json.Marshal(map[string]string{"type": "browse_dir", "path": tc.path, "request_id": tc.id})
		if err != nil {
			t.Fatal(err)
		}
		route(h, c, string(wire))
		for stage := 1; stage <= 2; stage++ {
			event := waitForType(t, c, "dir_listing")
			if event["path"] != tc.actual {
				t.Fatalf("canonical path changed: %v", event)
			}
			if tc.id == "" {
				if _, exists := event["request_id"]; exists {
					t.Fatalf("legacy request gained correlation: %v", event)
				}
				if _, exists := event["directory_stage"]; exists {
					t.Fatalf("legacy request gained stage: %v", event)
				}
			} else if event["request_id"] != tc.id || event["directory_stage"] != float64(stage) {
				t.Fatalf("original physical read/stage lost: %v", event)
			}
		}
	}
}

func TestDirectoryHelloAdvertisesExactCorrelationContract(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"phone"}`)
	hello := waitForType(t, c, "hello_ack")
	found := 0
	for _, capability := range hello["capabilities"].([]any) {
		if capability == "directory_request_correlation_v1" {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("correlation capability count=%d: %v", found, hello)
	}
}
