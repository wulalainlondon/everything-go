package clientproto

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"testing"
)

func TestMediaSessionStartAndReportWireProjection(t *testing.T) {
	var start protocol.Inbound
	if err := json.Unmarshal([]byte(`{"type":"codex_voice_start","session_id":"s_own","request_id":"native-start","voice_id":"own-voice","voice_media_capable":true,"voice_media_session_capable":true}`), &start); err != nil {
		t.Fatal(err)
	}
	got := NewAppV1().ParseCommand(start)
	if !got.VoiceMediaSessionCapable || !got.VoiceMediaCapable || got.RequestID != "native-start" {
		t.Fatal(got)
	}
	var report protocol.Inbound
	raw := `{"type":"codex_voice_media_state","session_id":"s_own","request_id":"native-start","voice_id":"own-voice","media_session_state":{"kind":"snapshot","operationId":"local_original","revision":0,"version":1,"observationAgeMs":-1}}`
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		t.Fatal(err)
	}
	got = NewAppV1().ParseCommand(report)
	if got.MediaSessionState == nil || got.MediaSessionState.ObservationAgeMS != -1 || got.RequestID != "native-start" {
		t.Fatal(got)
	}
}
