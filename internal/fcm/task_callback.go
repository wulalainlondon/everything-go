package fcm

import "fmt"

// NotifyTaskCallback addresses exactly one enrolled device. Stable event IDs
// retain the broker's idempotency receipt across uncertain network delivery.
func (n *Notifier) NotifyTaskCallback(authority, session, request, callback, invitation, device string, expires int64) {
	msg := v1message{allowedDevice: func(id string) bool { return id == device }}
	msg.Message.Data = map[string]string{"type": "task_callback_invite", "schema_version": fcmPayloadSchemaVersion, "authority_instance_id": authority, "session_id": session, "task_request_id": request, "callback_id": callback, "invitation_id": invitation, "expires_at": fmt.Sprint(expires), "event_id": invitation}
	msg.Message.Android = &v1androidConfig{Priority: "high", TTL: "60s", CollapseKey: invitation}
	targets := []target{}
	for _, dst := range n.targetsForPlatform("android") {
		if dst.deviceID == device {
			targets = append(targets, dst)
		}
	}
	n.sendTargets(msg, "task_callback_invite", targets)
}
