package netsvc

import (
	"context"
	"log"
	"net"
	"os"

	"golang.org/x/net/dns/dnsmessage"
)

// mDNS service identity — byte-for-byte the same names the Python bridge
// registers via zeroconf (_start_mdns_blocking): service "_bridge._tcp.local.",
// instance "bridge._bridge._tcp.local.", TXT version=2, port = WS port.
const (
	mdnsService  = "_bridge._tcp.local."
	mdnsInstance = "bridge._bridge._tcp.local."
	mdnsHost     = "bridge.local."
	mdnsTTL      = 120
)

var mdnsGroup = &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353}

// RegisterMDNS advertises the bridge over multicast DNS until ctx is cancelled.
// It answers queries for our service/instance/host names and sends one
// unsolicited announcement on startup. Best-effort: any setup failure is logged
// and the function returns (the UDP beacon is the app's primary discovery path).
func RegisterMDNS(ctx context.Context, port int, instanceName string) {
	ip, iface := firstIPv4Iface()
	if ip == nil {
		log.Printf("[mdns] no LAN IPv4, skipping")
		return
	}
	// Join on the interface that owns the LAN IP. With nil, Go picks the OS
	// default multicast interface, which on a multi-NIC mac (en0 + Tailscale
	// utun) is often the wrong one — queries arrive on the LAN NIC.
	conn, err := net.ListenMulticastUDP("udp4", iface, mdnsGroup)
	if err != nil {
		log.Printf("[mdns] listen failed: %v", err)
		return
	}
	defer conn.Close()
	_ = conn.SetReadBuffer(1 << 16)

	resp, err := buildMDNSAnnounce(ip, port)
	if err != nil {
		log.Printf("[mdns] pack failed: %v", err)
		return
	}

	if _, err := conn.WriteToUDP(resp, mdnsGroup); err != nil {
		log.Printf("[mdns] announce send failed: %v", err)
	}
	log.Printf("[mdns] advertising %s at %s:%d (version=2)", mdnsService, ip, port)

	go func() {
		<-ctx.Done()
		conn.Close() // unblocks ReadFromUDP below
	}()

	dbg := os.Getenv("EG_MDNS_DEBUG") == "1"
	buf := make([]byte, 9000)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			return // socket closed via ctx
		}
		match := mdnsQueryWantsUs(buf[:n])
		if dbg {
			log.Printf("[mdns] rx %dB from %s match=%v", n, src, match)
		}
		if match {
			_, _ = conn.WriteToUDP(resp, mdnsGroup)
		}
	}
}

// buildMDNSAnnounce packs the PTR/SRV/TXT/A answer set the bridge advertises.
func buildMDNSAnnounce(ip net.IP, port int) ([]byte, error) {
	var a4 [4]byte
	copy(a4[:], ip.To4())
	msg := dnsmessage.Message{
		Header: dnsmessage.Header{Response: true, Authoritative: true},
		Answers: []dnsmessage.Resource{
			{
				Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(mdnsService), Type: dnsmessage.TypePTR, Class: dnsmessage.ClassINET, TTL: mdnsTTL},
				Body:   &dnsmessage.PTRResource{PTR: dnsmessage.MustNewName(mdnsInstance)},
			},
			{
				Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(mdnsInstance), Type: dnsmessage.TypeSRV, Class: dnsmessage.ClassINET, TTL: mdnsTTL},
				Body:   &dnsmessage.SRVResource{Priority: 0, Weight: 0, Port: uint16(port), Target: dnsmessage.MustNewName(mdnsHost)},
			},
			{
				Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(mdnsInstance), Type: dnsmessage.TypeTXT, Class: dnsmessage.ClassINET, TTL: mdnsTTL},
				Body:   &dnsmessage.TXTResource{TXT: []string{"version=2"}},
			},
			{
				Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(mdnsHost), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: mdnsTTL},
				Body:   &dnsmessage.AResource{A: a4},
			},
		},
	}
	return msg.Pack()
}

// mdnsQueryWantsUs reports whether the packet is a query asking for any of our
// advertised names, so we only answer relevant questions.
func mdnsQueryWantsUs(pkt []byte) bool {
	var p dnsmessage.Parser
	if _, err := p.Start(pkt); err != nil {
		return false
	}
	for {
		q, err := p.Question()
		if err != nil {
			return false
		}
		switch q.Name.String() {
		case mdnsService, mdnsInstance, mdnsHost:
			return true
		}
	}
}

// firstIPv4 returns the first non-loopback IPv4 address, or nil.
func firstIPv4() net.IP {
	ip, _ := firstIPv4Iface()
	return ip
}

// firstIPv4Iface returns the first private (LAN) non-loopback IPv4 address and
// the interface that owns it, preferring RFC-1918 ranges over CGNAT/Tailscale
// (100.64/10) so mDNS joins the real LAN NIC.
func firstIPv4Iface() (net.IP, *net.Interface) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, nil
	}
	var fallbackIP net.IP
	var fallbackIf *net.Interface
	for i := range ifaces {
		ifi := &ifaces[i]
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			if ip4.IsPrivate() {
				return ip4, ifi // real LAN NIC (192.168/10.x/172.16)
			}
			if fallbackIP == nil {
				fallbackIP, fallbackIf = ip4, ifi
			}
		}
	}
	return fallbackIP, fallbackIf
}
