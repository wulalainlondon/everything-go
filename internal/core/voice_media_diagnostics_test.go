package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"strings"
	"testing"
	"time"
)

func inspectionFixture(t *testing.T) (*Hub, *Client, backend.SessionControlCaller, *voiceMediaSession) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, v := range h.voiceClients {
		owner = c
		v.mediaCapable = true
		v.mediaObserveCapable = true
	}
	owner.clientID = "actual-phone"
	owner.clientSurface = "android"
	r := &voiceMediaReceipt{settled: make(chan struct{}), result: backend.VoiceMediaResult{CommandID: "vm_" + strings.Repeat("a", 64), Sequence: 1, Action: "play", Endpoint: h.mediaEndpoint(owner)}}
	m := &voiceMediaSession{current: r, receipts: map[string]*voiceMediaReceipt{}, video: "M7lc1UVf-VE", state: "playing", volume: 35, observedAt: time.Now().Add(-time.Minute).UnixMilli()}
	h.voiceClients[owner].media = m
	return h, owner, caller, m
}
func TestMediaStatusRefreshIsBoundReadOnlyObservation(t *testing.T) {
	h, owner, caller, m := inspectionFixture(t)
	other := newTestClient(h)
	other.clientID = "usb-other-phone"
	done := make(chan any, 1)
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	json.Unmarshal(b, &command)
	if command.Action != "observe" || command.Sequence != 1 || command.CommandID != m.current.result.CommandID {
		t.Fatal(command)
	}
	volume := 47
	muted := false
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, MediaVideoID: command.VideoID, MediaState: "playing", MediaAudio: &protocol.MediaAudioInfo{DeviceModel: "samsung SM-N981", CommunicationProfile: "bluetooth_hfp", VoiceRoute: "bluetooth", PlayerMuted: &muted, SystemMediaMuted: &muted, SystemMediaVolume: &volume}}
	h.handleVoiceMediaState(other, ack)
	select {
	case <-done:
		t.Fatal("foreign observation accepted")
	default:
	}
	h.handleVoiceMediaState(owner, ack)
	r := (<-done).(backend.VoiceMediaResult)
	if !r.PlaybackConfirmed || r.AudioCheck != "bluetooth_call_media_unverified" || r.Endpoint.ClientID != "actual-phone" || r.Endpoint.Model != "samsung SM-N981" || r.LastObservedState != "playing" {
		t.Fatal(r)
	}
	if len(m.receipts) != 0 || m.sequence != 0 {
		t.Fatal("observation created a playback operation")
	}
	select {
	case <-other.send:
		t.Fatal("queried other phone")
	default:
	}
}
func TestMediaOldClientPreservesLastKnownTerminalWithoutInventingCurrentState(t *testing.T) {
	h, owner, caller, m := inspectionFixture(t)
	h.voiceClients[owner].mediaObserveCapable = false
	m.state = "ended"
	v, e := h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if e != nil {
		t.Fatal(e)
	}
	r := v.(backend.VoiceMediaResult)
	if r.State != "unverified" || r.LastObservedState != "ended" || r.ObservationAgeMS == nil || *r.ObservationAgeMS < 30000 || r.PlaybackConfirmed || r.Endpoint.ClientID != "actual-phone" {
		t.Fatal(r)
	}
	select {
	case <-owner.send:
		t.Fatal("old client got unknown observe action")
	default:
	}
}
func TestMediaDiagnosticsRejectClaimsOfActualPredictedRoutes(t *testing.T) {
	for _, a := range []*protocol.MediaAudioInfo{{MediaRoutingObservation: "actual"}, {DeviceModel: "x\nwrong"}, {CommunicationProfile: "a2dp_actual"}, {PredictedMediaRoutes: []string{"invented"}}} {
		if validMediaAudio(a) {
			t.Fatal(a)
		}
	}
	if !validMediaAudio(&protocol.MediaAudioInfo{CommunicationProfile: "bluetooth_hfp", CommunicationDevice: "Carmax BT", PredictedMediaRoutes: []string{"bluetooth_media"}, MediaRoutingObservation: "predicted"}) {
		t.Fatal("valid read-only diagnostic rejected")
	}
}
func TestMediaEndpointIdentifiesNativePhoneBeforeAnySong(t *testing.T) {
	h, owner, caller, _ := inspectionFixture(t)
	h.voiceClients[owner].media = nil
	h.voiceClients[owner].mediaDeviceModel = "samsung SM-N981"
	v, e := h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if e != nil {
		t.Fatal(e)
	}
	r := v.(backend.VoiceMediaResult)
	if r.Endpoint.Model != "samsung SM-N981" || r.Endpoint.VoiceID != caller.VoiceID || r.Endpoint.ClientID != "actual-phone" || r.Endpoint.Verification != "bound_current_call" || r.PlaybackConfirmed {
		t.Fatal(r)
	}
}
