package fcm

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"golang.org/x/oauth2"
)

func testNotifier(path, endpoint string, client *http.Client) *Notifier {
	return &Notifier{
		projectID:    "test",
		tokenSource:  oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "oauth"}),
		registryPath: path,
		endpoint:     endpoint,
		http:         client,
		devices:      make(map[string]deviceRegistration),
	}
}

func TestPerDeviceRegistryPersistsWithoutOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fcm_tokens.json")
	n := testNotifier(path, "", http.DefaultClient)
	n.SetToken("note20", "token-note20")
	n.SetToken("ipad", "token-ipad")

	reloaded := testNotifier(path, "", http.DefaultClient)
	reloaded.loadRegistry()
	targets := reloaded.targets()
	sort.Slice(targets, func(i, j int) bool { return targets[i].deviceID < targets[j].deviceID })
	if len(targets) != 2 || targets[0].deviceID != "ipad" || targets[0].token != "token-ipad" ||
		targets[1].deviceID != "note20" || targets[1].token != "token-note20" {
		t.Fatalf("registry did not preserve both devices: %+v", targets)
	}
}

func TestTokenMovesToNewDeviceWithoutDuplicatePush(t *testing.T) {
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), "", http.DefaultClient)
	n.SetToken("old-install", "same-token")
	n.SetToken("new-install", "same-token")
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != "new-install" {
		t.Fatalf("token was not moved to the new stable device: %+v", targets)
	}
}

func TestLegacySingleTokenIsImported(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fcm_token.txt"), []byte("legacy-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	n := testNotifier(filepath.Join(dir, "fcm_tokens.json"), "", http.DefaultClient)
	n.loadRegistry()
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != legacyDeviceID || targets[0].token != "legacy-token" {
		t.Fatalf("legacy token was not imported: %+v", targets)
	}
	if _, err := os.Stat(filepath.Join(dir, "fcm_tokens.json")); err != nil {
		t.Fatalf("imported registry was not persisted: %v", err)
	}
}

func TestNotifyFansOutAndInvalidatesOnlyFailedDevice(t *testing.T) {
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var msg v1message
		_ = json.Unmarshal(body, &msg)
		mu.Lock()
		received = append(received, msg.Message.Token)
		mu.Unlock()
		if msg.Message.Token == "bad-token" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"status":"UNREGISTERED"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "good-token")
	n.SetToken("old-phone", "bad-token")
	n.NotifyTaskDone("session", "done", "s1")

	mu.Lock()
	sort.Strings(received)
	gotReceived := append([]string(nil), received...)
	mu.Unlock()
	if strings.Join(gotReceived, ",") != "bad-token,good-token" {
		t.Fatalf("notification did not fan out once per token: %v", gotReceived)
	}
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != "note20" || targets[0].token != "good-token" {
		t.Fatalf("fatal response removed the wrong registrations: %+v", targets)
	}
}

func TestWorkAttentionCarriesStableDedupFacts(t *testing.T) {
	var got v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "token")
	n.NotifyWorkAttention("morrie", "wi1", "Review release", 42, "review_ready")
	data := got.Message.Data
	if data["type"] != "work_attention" || data["authority_instance_id"] != "morrie" ||
		data["work_item_id"] != "wi1" || data["activity_revision"] != "42" || data["kind"] != "review_ready" {
		t.Fatalf("work attention payload=%+v", data)
	}
}

func TestExternalEventCarriesCanonicalIdentity(t *testing.T) {
	var got v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "token")
	n.NotifyExternalEvent("wulala", "evt_1", "github", "ci_failed", "error", "CI failed", "race test")
	data := got.Message.Data
	if data["type"] != "external_event" || data["authority_instance_id"] != "wulala" ||
		data["event_id"] != "evt_1" || data["source"] != "github" || data["kind"] != "ci_failed" {
		t.Fatalf("external event payload=%+v", data)
	}
}

func TestSummarizeStripsMarkdown(t *testing.T) {
	got := summarize("Here is **bold** and `code` and a [link](http://x).")
	if strings.ContainsAny(got, "*`") {
		t.Fatalf("markdown marks not stripped: %q", got)
	}
	if !strings.Contains(got, "link") || strings.Contains(got, "http://x") {
		t.Fatalf("link text should remain, URL should go: %q", got)
	}
}

func TestSummarizeUsesLastParagraph(t *testing.T) {
	got := summarize("first paragraph\n\nsecond paragraph is the summary.")
	if !strings.HasPrefix(got, "second paragraph") {
		t.Fatalf("should summarize the last paragraph, got %q", got)
	}
}

func TestSummarizeFirstSentenceOnly(t *testing.T) {
	got := summarize("Done. More detail follows here that should be dropped.")
	if got != "Done." {
		t.Fatalf("should keep only the first sentence, got %q", got)
	}
}

func TestSummarizeCJKSentenceWithSpace(t *testing.T) {
	// The Python bridge's split requires whitespace after the punctuation
	// ((?<=[。！？!?.])\s+), so a space-separated CJK sentence splits.
	got := summarize("完成了。 後面這句應該被丟掉。")
	if got != "完成了。" {
		t.Fatalf("CJK first-sentence split (with space) failed, got %q", got)
	}
}

func TestSummarizeCJKNoSpaceMatchesPython(t *testing.T) {
	// Without trailing whitespace there is nothing for \s+ to match, so — exactly
	// like the Python bridge — the whole text is kept (no CJK sentence split).
	got := summarize("完成了。後面這句也留著。")
	if got != "完成了。後面這句也留著。" {
		t.Fatalf("CJK without space should keep full text (Python parity), got %q", got)
	}
}

func TestSummarizeCapsAt120Runes(t *testing.T) {
	long := strings.Repeat("漢", 200) // 200 CJK runes, no sentence breaks
	got := summarize(long)
	r := []rune(got)
	// 120 runes + the ellipsis.
	if len(r) != 121 || r[120] != '…' {
		t.Fatalf("expected 120 runes + ellipsis, got %d runes (last=%q)", len(r), string(r[len(r)-1]))
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if got := summarize(""); got != "" {
		t.Fatalf("empty input should summarize to empty, got %q", got)
	}
}

func TestTokenFatal(t *testing.T) {
	if !tokenFatal(404, nil) {
		t.Fatal("404 should be fatal")
	}
	if !tokenFatal(400, []byte(`{"error":{"status":"INVALID_ARGUMENT"}}`)) {
		t.Fatal("400 INVALID_ARGUMENT should be fatal")
	}
	if !tokenFatal(404, []byte("UNREGISTERED")) {
		t.Fatal("UNREGISTERED should be fatal")
	}
	if tokenFatal(503, []byte("temporary")) {
		t.Fatal("503 transient should NOT be fatal")
	}
}
