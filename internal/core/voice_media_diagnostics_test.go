package core

import (
	"everything-go/internal/protocol"
	"testing"
)

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
