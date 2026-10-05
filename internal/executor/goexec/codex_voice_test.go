package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

func TestRealtimeNotificationsAreBoundToExactThreadAndVoice(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	call := &codexVoiceCall{voiceID: "voice-one", threadID: "native-one", sessionID: "one", answer: make(chan backend.RealtimeVoiceAnswer, 1), failure: make(chan error, 1), closed: make(chan struct{})}
	c.voiceCalls = map[string]*codexVoiceCall{"native-one": call}
	for _, value := range []string{`{"threadId":"native-other","sdp":"private-answer"}`, `{"threadId":"native-one","realtimeSessionId":"old-voice","sdp":"private-answer"}`} {
		c.dispatchVoice("thread/realtime/sdp", json.RawMessage(value))
	}
	select {
	case <-call.answer:
		t.Fatal("accepted another thread/voice answer")
	default:
	}
	c.dispatchVoice("thread/realtime/sdp", json.RawMessage(`{"threadId":"native-one","sdp":"v=0\r\nanswer"}`))
	answer := <-call.answer
	if answer.ThreadID != "native-one" || answer.VoiceID != "voice-one" {
		t.Fatalf("wrong voice binding")
	}
	c.dispatchVoice("thread/realtime/closed", json.RawMessage(`{"threadId":"native-one","reason":"requested"}`))
	select {
	case <-call.closed:
	default:
		t.Fatal("no closure")
	}
	if c.voiceCalls["native-one"] != nil {
		t.Fatal("closed voice retained")
	}
}

func TestVoiceStopOwnershipSurvivesNativeIDChangeButTextCannotSteerOldThread(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("one", "one", t.TempDir(), backend.Codex, "", "", "new-native")
	call := &codexVoiceCall{voiceID: "voice-one", threadID: "old-native", sessionID: "one"}
	c.voiceCalls = map[string]*codexVoiceCall{"old-native": call}
	if c.ownedVoice(s, "voice-one") != call {
		t.Fatal("lost original voice stop binding")
	}
	if c.ownedVoice(s, "different-voice") != nil {
		t.Fatal("accepted another voice")
	}
	other := session.NewRegistry().Create("other", "other", t.TempDir(), backend.Codex, "", "", "old-native")
	if c.ownedVoice(other, "voice-one") != nil {
		t.Fatal("accepted another session")
	}
}
