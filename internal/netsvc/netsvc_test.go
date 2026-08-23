package netsvc

import (
	"encoding/json"
	"net"
	"testing"

	"golang.org/x/net/dns/dnsmessage"
)

// TestBeaconPayloadSchema pins the announce datagram to the app's
// DiscoveryAnnounceSchema (app/src/schemas/discovery.ts): exact magic/type,
// required fields, ips.min(1), and the ≤512 B size cap.
func TestBeaconPayloadSchema(t *testing.T) {
	b := &Beacon{wsPort: 8767, discoPort: 8767, instanceID: "everything-go", hostname: "host.local"}
	raw := b.payload([]string{"192.168.1.42", "100.1.2.3"})
	if len(raw) > maxBeaconBytes {
		t.Fatalf("payload %d B exceeds %d cap", len(raw), maxBeaconBytes)
	}
	var a announce
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.Magic != "CLAUDE_BRIDGE_DISCOVERY_V1" {
		t.Errorf("magic = %q", a.Magic)
	}
	if a.Type != "announce" {
		t.Errorf("type = %q", a.Type)
	}
	if a.WSPort != 8767 {
		t.Errorf("ws_port = %d", a.WSPort)
	}
	if len(a.IPs) < 1 {
		t.Errorf("ips must have ≥1 entry (schema), got %v", a.IPs)
	}
	if a.Version != beaconVersion || a.InstanceID == "" || a.Hostname == "" || a.TS == 0 {
		t.Errorf("missing required fields: %+v", a)
	}
}

// TestBeaconPayloadNeverEmptyIPs guarantees we never emit ips:[] (schema rejects
// it), falling back to loopback when no interface is detected.
func TestBeaconPayloadNeverEmptyIPs(t *testing.T) {
	b := &Beacon{wsPort: 8766, instanceID: "x", hostname: "h"}
	var a announce
	if err := json.Unmarshal(b.payload(nil), &a); err != nil {
		t.Fatal(err)
	}
	if len(a.IPs) < 1 {
		t.Fatalf("empty ips would fail schema validation")
	}
}

// TestDirectedBroadcast verifies broadcast is derived from the real netmask,
// not a class-C guess — the bug that made .68.255 (a host on a /22) fail.
func TestDirectedBroadcast(t *testing.T) {
	cases := []struct {
		ip   string
		bits int
		want string
	}{
		{"192.168.68.50", 22, "192.168.71.255"}, // /22 — the real failing case
		{"192.168.1.42", 24, "192.168.1.255"},   // /24 — class-C still correct
		{"10.0.0.5", 8, "10.255.255.255"},       // /8
		{"172.16.5.9", 20, "172.16.15.255"},     // /20
	}
	for _, c := range cases {
		ip := net.ParseIP(c.ip).To4()
		mask := net.CIDRMask(c.bits, 32)
		if got := directedBroadcast(ip, mask); got != c.want {
			t.Errorf("%s/%d: got %s, want %s", c.ip, c.bits, got, c.want)
		}
	}
}

// TestMDNSAnnounceRecords unpacks the advertised packet and checks it carries
// the PTR/SRV/TXT/A answer set with the Python-parity names + version=2 TXT.
func TestMDNSAnnounceRecords(t *testing.T) {
	raw, err := buildMDNSAnnounce(net.IPv4(192, 168, 1, 42), 8767)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	var msg dnsmessage.Message
	if err := msg.Unpack(raw); err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if !msg.Header.Response {
		t.Error("expected response bit set")
	}
	var ptr, srv, txt, a bool
	for _, r := range msg.Answers {
		switch body := r.Body.(type) {
		case *dnsmessage.PTRResource:
			ptr = r.Header.Name.String() == mdnsService && body.PTR.String() == mdnsInstance
		case *dnsmessage.SRVResource:
			srv = r.Header.Name.String() == mdnsInstance && body.Port == 8767 && body.Target.String() == mdnsHost
		case *dnsmessage.TXTResource:
			txt = len(body.TXT) == 1 && body.TXT[0] == "version=2"
		case *dnsmessage.AResource:
			a = r.Header.Name.String() == mdnsHost && body.A == [4]byte{192, 168, 1, 42}
		}
	}
	if !ptr || !srv || !txt || !a {
		t.Errorf("missing record(s): ptr=%v srv=%v txt=%v a=%v", ptr, srv, txt, a)
	}
}

// TestMDNSQueryMatch confirms we answer only queries for our advertised names.
func TestMDNSQueryMatch(t *testing.T) {
	build := func(name string) []byte {
		b := dnsmessage.NewBuilder(nil, dnsmessage.Header{})
		_ = b.StartQuestions()
		_ = b.Question(dnsmessage.Question{
			Name:  dnsmessage.MustNewName(name),
			Type:  dnsmessage.TypePTR,
			Class: dnsmessage.ClassINET,
		})
		out, _ := b.Finish()
		return out
	}
	if !mdnsQueryWantsUs(build(mdnsService)) {
		t.Error("should match service query")
	}
	if !mdnsQueryWantsUs(build(mdnsInstance)) {
		t.Error("should match instance query")
	}
	if mdnsQueryWantsUs(build("_http._tcp.local.")) {
		t.Error("should NOT match unrelated query")
	}
}
