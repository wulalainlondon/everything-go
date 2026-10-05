package core

import (
	"context"
	"regexp"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

var voiceIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{8,100}$`)

type clientVoiceCall struct {
	session            *session.Session
	voiceID, requestID string
	cancel             context.CancelFunc
	stopping           bool
}

func voiceEvent(cmd clientproto.Command, state, message string) protocol.RealtimeVoiceEvent {
	return protocol.RealtimeVoiceEvent{Type: "codex_voice_event", SessionID: cmd.SessionID, RequestID: cmd.RequestID, VoiceID: cmd.VoiceID, State: state, Message: message}
}
func (h *Hub) ownsVoice(c *Client, call *clientVoiceCall) bool {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	return h.voiceClients[c] == call
}

func (h *Hub) handleVoiceStart(c *Client, cmd clientproto.Command) {
	if !backend.ValidRealtimeVoiceName(cmd.VoiceName) {
		c.enqueueEvent(voiceEvent(cmd, "error", "此音色不支援目前的語音版本"))
		return
	}
	if !voiceIdentifier.MatchString(cmd.VoiceID) || !voiceIdentifier.MatchString(cmd.RequestID) || len(cmd.SDP) > 64*1024 {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音請求格式無效"))
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok || s.Backend() != backend.Codex || s.ResumeID() == "" || h.cfg.CodexRemote == "" {
		c.enqueueEvent(voiceEvent(cmd, "error", "此對話沒有可使用的 Codex thread"))
		return
	}
	if cmd.ThreadID != "" && cmd.ThreadID != s.ResumeID() {
		c.enqueueEvent(voiceEvent(cmd, "error", "對話對應已變更，請重新開啟後再連線"))
		return
	}
	if !h.controls.MobileMayWrite(s.ID) {
		c.enqueueEvent(voiceEvent(cmd, "error", "此對話由桌面控制，請先取回手機控制权"))
		return
	}
	exec, ok := h.exec.(backend.RealtimeVoiceExecutor)
	if !ok {
		c.enqueueEvent(voiceEvent(cmd, "error", "此 Bridge 不支援雙向語音"))
		return
	}
	parent := c.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	call := &clientVoiceCall{session: s, voiceID: cmd.VoiceID, requestID: cmd.RequestID, cancel: cancel}
	h.voiceMu.Lock()
	if h.voiceClients == nil {
		h.voiceClients = map[*Client]*clientVoiceCall{}
	}
	if h.voiceClients[c] != nil {
		h.voiceMu.Unlock()
		cancel()
		c.enqueueEvent(voiceEvent(cmd, "error", "請先結束上一個語音對話"))
		return
	}
	h.voiceClients[c] = call
	h.voiceMu.Unlock()
	c.enqueueEvent(voiceEvent(cmd, "starting", ""))
	go func() {
		defer cancel()
		answer, err := exec.StartRealtimeVoice(ctx, s, backend.RealtimeVoiceStart{VoiceID: cmd.VoiceID, VoiceName: cmd.VoiceName, SDP: cmd.SDP, OnEvent: func(event backend.RealtimeVoiceEvent) {
			if !c.live() || !h.ownsVoice(c, call) {
				return
			}
			out := voiceEvent(cmd, event.State, event.Message)
			out.Text = event.Text
			out.Role = event.Role
			out.ThreadID = s.ResumeID()
			c.enqueueEvent(out)
			if event.State == "closed" {
				h.voiceMu.Lock()
				if h.voiceClients[c] == call {
					delete(h.voiceClients, c)
				}
				h.voiceMu.Unlock()
			}
		}})
		if !h.ownsVoice(c, call) {
			cleanup, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			_ = exec.StopRealtimeVoice(cleanup, s, call.voiceID)
			return
		}
		if err != nil {
			h.voiceMu.Lock()
			if h.voiceClients[c] == call {
				delete(h.voiceClients, c)
			}
			h.voiceMu.Unlock()
			if c.live() {
				c.enqueueEvent(voiceEvent(cmd, "error", err.Error()))
			}
			return
		}
		if answer.ThreadID != s.ResumeID() || answer.VoiceID != call.voiceID {
			h.stopClientVoice(c, call)
			c.enqueueEvent(voiceEvent(cmd, "error", "語音回應與目前對話不一致"))
			return
		}
		if !c.live() {
			h.stopClientVoice(c, call)
			return
		}
		out := voiceEvent(cmd, "answer", "")
		out.ThreadID = answer.ThreadID
		out.SDP = answer.SDP
		c.enqueueEvent(out)
	}()
}
func (h *Hub) stopClientVoice(c *Client, call *clientVoiceCall) {
	h.voiceMu.Lock()
	if call.stopping {
		h.voiceMu.Unlock()
		return
	}
	call.stopping = true
	h.voiceMu.Unlock()
	call.cancel()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		var err error
		if exec, ok := h.exec.(backend.RealtimeVoiceExecutor); ok {
			err = exec.StopRealtimeVoice(ctx, call.session, call.voiceID)
		}
		h.voiceMu.Lock()
		call.stopping = false
		if err == nil && h.voiceClients[c] == call {
			delete(h.voiceClients, c)
		}
		h.voiceMu.Unlock()
		if c.live() {
			cmd := clientproto.Command{SessionID: call.session.ID, RequestID: call.requestID, VoiceID: call.voiceID}
			if err != nil {
				c.enqueueEvent(voiceEvent(cmd, "error", "未能確認語音已結束，請再次點結束語音"))
			} else {
				c.enqueueEvent(voiceEvent(cmd, "closed", ""))
			}
		}
	}()
}
func (h *Hub) cleanupClientVoice(c *Client) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call != nil {
		h.stopClientVoice(c, call)
	}
}
func (h *Hub) handleVoiceStop(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call == nil {
		c.enqueueEvent(voiceEvent(cmd, "closed", ""))
		return
	}
	if call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音停止請求不屬於目前連線"))
		return
	}
	c.enqueueEvent(voiceEvent(cmd, "stopping", ""))
	h.stopClientVoice(c, call)
}
func (h *Hub) handleVoiceText(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call == nil || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || !h.controls.MobileMayWrite(cmd.SessionID) {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音對話已失效或沒有寫入權限"))
		return
	}
	exec, ok := h.exec.(backend.RealtimeVoiceExecutor)
	if !ok {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if err := exec.AppendRealtimeVoiceText(ctx, call.session, call.voiceID, cmd.Content); err != nil && c.live() {
			c.enqueueEvent(voiceEvent(cmd, "error", "語音文字未能送出；不會自動重送"))
		}
	}()
}
