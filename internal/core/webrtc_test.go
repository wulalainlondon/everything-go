package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"everything-go/internal/protocol"
)

// TestWebRTCPromotesDataChannelToClient exercises the whole P2P path in-process:
// a real Pion client offers a "bridge" DataChannel; the hub answers (baking ICE
// into the answer, no STUN — loopback host candidates only); once the channel
// opens the hub promotes it to a full client; a hello sent over the DataChannel
// is answered with hello_ack over that same DataChannel. This proves the
// signaling protocol AND that the handshake + route loop runs unchanged over a
// DataChannel transport.
func TestWebRTCPromotesDataChannelToClient(t *testing.T) {
	h, _ := newTestHub(t)
	h.SetICEServers(nil) // loopback host candidates → no network, fast + deterministic

	sig := newTestClient(h) // signaling client; its send channel captures answers

	// --- Client peer (the app's role) ---
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client NewPeerConnection: %v", err)
	}
	defer clientPC.Close()

	dc, err := clientPC.CreateDataChannel("bridge", nil)
	if err != nil {
		t.Fatalf("CreateDataChannel: %v", err)
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	inbound := make(chan string, 8)
	dc.OnMessage(func(m webrtc.DataChannelMessage) { inbound <- string(m.Data) })

	// Create the offer and bake the client's candidates (non-trickle for the
	// test — both sides carry full candidate sets in their SDP).
	offer, err := clientPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	clientGathered := webrtc.GatheringCompletePromise(clientPC)
	if err := clientPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("client SetLocalDescription: %v", err)
	}
	<-clientGathered

	// --- Feed the offer through the router, exactly like an inbound frame ---
	offerFrame, _ := json.Marshal(map[string]any{
		"type": "webrtc_offer",
		"sdp":  clientPC.LocalDescription().SDP,
	})
	in, err := protocol.ParseInbound(offerFrame)
	if err != nil {
		t.Fatalf("ParseInbound: %v", err)
	}
	h.route(context.Background(), sig, in) // blocks until the answer is gathered

	answer := waitForType(t, sig, "webrtc_answer")
	sdp, _ := answer["sdp"].(string)
	if sdp == "" {
		t.Fatal("webrtc_answer carried no sdp")
	}
	if err := clientPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: sdp,
	}); err != nil {
		t.Fatalf("client SetRemoteDescription: %v", err)
	}

	// --- DataChannel must open, then carry the bridge protocol ---
	select {
	case <-opened:
	case <-time.After(8 * time.Second):
		t.Fatal("client DataChannel never opened")
	}

	if err := dc.SendText(`{"type":"hello","device_id":"dweb"}`); err != nil {
		t.Fatalf("send hello over DC: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case raw := <-inbound:
			var m map[string]any
			if err := json.Unmarshal([]byte(raw), &m); err != nil {
				t.Fatalf("bad frame over DC: %v", err)
			}
			if m["type"] == "hello_ack" {
				if m["device_id"] != "dweb" {
					t.Fatalf("hello_ack device_id = %v, want dweb", m["device_id"])
				}
				return // full P2P loop proven
			}
		case <-deadline:
			t.Fatal("no hello_ack received over the DataChannel")
		}
	}
}

// TestWebRTCOfferMissingSDP rejects an offer with no SDP rather than spinning up
// a peer connection.
func TestWebRTCOfferMissingSDP(t *testing.T) {
	h, _ := newTestHub(t)
	sig := newTestClient(h)
	h.route(context.Background(), sig, protocol.Inbound{Type: "webrtc_offer"})
	ev := waitForType(t, sig, "error")
	if ev["code"] != "webrtc_offer_invalid" {
		t.Fatalf("error code = %v, want webrtc_offer_invalid", ev["code"])
	}
	if sig.rtc != nil {
		t.Fatal("no peer connection should be created for an invalid offer")
	}
}

// TestWebRTCICEWithoutPeerIsNoop ensures a stray webrtc_ice (no prior offer)
// is ignored without panicking.
func TestWebRTCICEWithoutPeerIsNoop(t *testing.T) {
	h, _ := newTestHub(t)
	sig := newTestClient(h)
	h.route(context.Background(), sig, protocol.Inbound{Type: "webrtc_ice", Candidate: "candidate:bogus"})
	if sig.rtc != nil {
		t.Fatal("webrtc_ice must not create a peer")
	}
}
