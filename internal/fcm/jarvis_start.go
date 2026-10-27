package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type StartReceipt struct {
	State           string `json:"state"`
	ProviderReceipt string `json:"provider_receipt,omitempty"`
	DeviceReceived  string `json:"device_received"`
	Displayed       string `json:"displayed"`
	Opened          string `json:"opened"`
	ExplicitRead    string `json:"explicit_read"`
}

// SendJarvisStart is a single bounded send; durable outbox ownership is core's.
// Native schema1 registration is checked by core, never inferred from old FCM.
func (n *Notifier) SendJarvisStart(ctx context.Context, operation, authority, manager, session, request, event, device string, expires int64, guard func() bool) StartReceipt {
	receipt := StartReceipt{State: "skipped", DeviceReceived: "unknown", Displayed: "unknown", Opened: "unknown", ExplicitRead: "unknown"}
	if n == nil || expires <= time.Now().UnixMilli() || guard == nil || !guard() {
		return receipt
	}
	var dst target
	platform := ""
	for _, p := range []string{"android", "ios"} {
		for _, d := range n.targetsForPlatform(p) {
			if d.deviceID == device {
				dst = d
				platform = p
			}
		}
	}
	if dst.deviceID == "" {
		return receipt
	}
	msg := v1message{allowedDevice: func(id string) bool { return id == device && guard() }}
	msg.Message.Token = dst.token
	msg.Message.Data = map[string]string{"type": "jarvis_work_start", "schema_version": "1", "authority_instance_id": authority, "manager_session_id": manager, "session_id": session, "task_request_id": request, "event_id": event, "expires_at": fmt.Sprint(expires)}
	if platform == "android" {
		msg.Message.Android = &v1androidConfig{Priority: "high", TTL: "180s", CollapseKey: event}
	} else {
		msg.Message.APNS = visibleAPNSConfig("", shortStableID(authority, manager), shortStableID(event), &v1notification{Title: "Averything", Body: "工作狀態有更新"})
		msg.Message.APNS.Headers["apns-expiration"] = fmt.Sprint(expires / 1000)
	}
	filtered, allowed := n.messageForDevice(msg, "work_attention", dst)
	if !allowed {
		return receipt
	}
	body, e := json.Marshal(filtered)
	if e != nil {
		return receipt
	}
	if n.relay != nil {
		e = n.relay.SendFCMOnce(ctx, operation, device, dst.token, body, func() bool { _, ok := n.messageForDevice(msg, "work_attention", dst); return ok && guard() })
		if e == nil {
			receipt.State = "provider_accepted"
			receipt.ProviderReceipt = operation
		} else {
			receipt.State = "unknown"
		}
		return receipt
	}
	if n.tokenSource == nil || n.http == nil {
		return receipt
	}
	tok, e := n.tokenSource.Token()
	if e != nil {
		return receipt
	}
	if !guard() {
		return receipt
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, n.endpoint, bytes.NewReader(body))
	if e != nil {
		return receipt
	}
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, e := n.http.Do(req)
	if e != nil {
		receipt.State = "unknown"
		return receipt
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 32769))
	if e != nil || len(raw) > 32768 {
		receipt.State = "unknown"
		return receipt
	}
	if resp.StatusCode == 200 {
		var ack struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &ack) == nil && ack.Name != "" {
			receipt.State = "provider_accepted"
			receipt.ProviderReceipt = ack.Name
		} else {
			receipt.State = "unknown"
		}
	} else {
		receipt.State = "failed"
	}
	return receipt
}
