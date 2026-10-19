package fcm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCallbackPushTargetsOneDeviceAndCarriesNoResultOrCredentials(t *testing.T) {
	var messages []v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m v1message
		if e := json.NewDecoder(r.Body).Decode(&m); e != nil {
			t.Error(e)
		}
		messages = append(messages, m)
		w.Write([]byte(`{"name":"sent"}`))
	}))
	defer server.Close()
	n := testNotifier("", server.URL, server.Client())
	n.SetToken("s10", "token-s10", "android")
	n.SetToken("note20", "token-note20", "android")
	n.SetToken("ios", "token-ios", "ios")
	n.NotifyTaskCallback("authority", "session", "request", "cb_policy", "cb_invitation", "s10", time.Now().Add(time.Minute).UnixMilli())
	if len(messages) != 1 {
		t.Fatal(len(messages))
	}
	m := messages[0]
	if m.Message.Token != "token-s10" || m.Message.Android.TTL != "60s" || m.Message.Notification != nil {
		t.Fatal(m)
	}
	for _, key := range []string{"body", "report", "text", "auth_token", "reply_capability"} {
		if _, ok := m.Message.Data[key]; ok {
			t.Fatal("sensitive push field", key)
		}
	}
}
