package fcm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestJarvisStartPushIsGenericDataOnlyAndSingleAttempt(t *testing.T) {
	for _, platform := range []string{"android", "ios"} {
		t.Run(platform, func(t *testing.T) {
			count := 0
			var got v1message
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count++
				_ = json.NewDecoder(r.Body).Decode(&got)
				_ = json.NewEncoder(w).Encode(map[string]string{"name": "projects/test/messages/exact"})
			}))
			defer server.Close()
			n := testNotifier(filepath.Join(t.TempDir(), "tokens.json"), server.URL, server.Client())
			n.SetToken("phone", "fixture-token", platform)
			r := n.SendJarvisStart(context.Background(), "op", "i", "manager", "session", "request", "start_exact", "phone", time.Now().Add(time.Minute).UnixMilli(), func() bool { return true })
			if r.State != "provider_accepted" || r.Displayed != "unknown" || count != 1 {
				t.Fatal(r, count)
			}
			if got.Message.Data["schema_version"] != "1" || got.Message.Data["type"] != "jarvis_work_start" {
				t.Fatal(got.Message.Data)
			}
			if platform == "android" && (got.Message.Notification != nil || got.Message.APNS != nil) {
				t.Fatal("background native filter bypass")
			}
			if platform == "ios" && (got.Message.APNS == nil || got.Message.APNS.Payload.APS.Alert.Body != "工作狀態有更新") {
				t.Fatal("lockscreen private leakage", got.Message.APNS)
			}
			n.SendJarvisStart(context.Background(), "op", "i", "manager", "session", "request", "start_exact", "phone", time.Now().Add(time.Minute).UnixMilli(), func() bool { return false })
			if count != 1 {
				t.Fatal("revoked send issued")
			}
		})
	}
}
