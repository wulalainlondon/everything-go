package goexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"io"
	"strings"
	"time"
)

func (c *Codex) applyVoiceMediaTools(s *session.Session, params map[string]any) {
	p, ok := c.controlProvider.(backend.VoiceMediaProvider)
	if !ok || !p.VoiceMediaEnabled() || s == nil || strings.HasPrefix(s.ID, "s_dg_") {
		return
	}
	fields := map[string]any{
		"url":   map[string]any{"type": "string", "maxLength": 2048, "description": "Real user-supplied or verified HTTPS YouTube video, canonical YouTube Music recording, or native Apple Music song URL. Never invent a song ID or resolve by dispatching a search."},
		"title": map[string]any{"type": "string", "maxLength": 200, "description": "For native Music, exact song title only, never an artist-title display label. YouTube retains title label semantics."}, "artist": map[string]any{"type": "string", "maxLength": 200, "description": "Exact native Music artist. Never guess a localized alias."},
		"display_label":     map[string]any{"type": "string", "maxLength": 200, "description": "Display only. Never a native title, artist, item ID or matching expectation."},
		"action":            map[string]any{"type": "string", "enum": []string{"pause", "resume", "stop", "next", "previous", "seek", "volume"}},
		"source":            map[string]any{"type": "string", "enum": []string{"youtube", "native_music"}},
		"position_ms":       map[string]any{"type": "integer", "minimum": 0, "maximum": 9007199254740991},
		"volume":            map[string]any{"type": "integer", "minimum": 0, "maximum": 100},
		"expected_revision": map[string]any{"type": "integer", "minimum": 0, "maximum": 9007199254740991},
		"adopt_current":     map[string]any{"type": "boolean", "description": "Only true after the user explicitly consents to pause this exact fresh identified foreign Music item. Never infer consent."},
		"expected_media_id": map[string]any{"type": "string", "maxLength": 200},
		"intent":            map[string]any{"type": "string", "pattern": "^vop_[A-Za-z0-9_-]{8,100}$", "description": "Original intent. Do not mint a retry intent after uncertainty."},
		"operation":         map[string]any{"type": "string", "pattern": "^vop_[A-Za-z0-9_-]{8,100}$", "description": "Exact original operation to observe without rerouting or replaying it."},
	}
	defs := []struct {
		name, description string
		props, required   []string
	}{
		{"play_youtube", "User-requested official visible YouTube playback on this exact call. A capable native coordinator performs the source switch once. Queued is not playing or audible. Foreign Music requires explicit adoption; never auto-pause it.", []string{"url", "title", "display_label", "expected_revision", "adopt_current", "expected_media_id"}, []string{"url"}},
		{"play_music", "User-requested native Music on this exact call: Apple Music on iOS, official YouTube Music on Android. Supply a verified exact song URL, exact song title (without artist prefix) and verified artist separately; display_label is presentation only. Permission/subscription/foreground refusals are not playback. Never search or buy through dispatch.", []string{"url", "title", "artist", "display_label", "expected_revision", "adopt_current", "expected_media_id"}, []string{"url"}},
		{"control", "Control the explicit source on this exact call. seek uses position_ms; volume applies only to the current YouTube coordinator, never native Music or assistant/system gain. Receipt causality and local snapshots are separate.", []string{"action", "source", "position_ms", "volume", "expected_revision", "adopt_current", "expected_media_id"}, []string{"action"}},
		{"music_control", "Control native Music on this exact call: pause/resume/stop/next/previous/seek. Availability depends on the native binding; no native Music gain setter.", []string{"action", "position_ms", "expected_revision", "adopt_current", "expected_media_id"}, []string{"action"}},
		{"status", "Read-only current session observation, or the exact original operation with operation. Never replays playback, changes provider, or treats a local snapshot as completion of a remote command. Fresh playing is not proof of human audibility.", []string{"source", "operation"}, nil},
	}

	tools := []map[string]any{}
	for _, d := range defs {
		props := map[string]any{}
		for _, name := range d.props {
			props[name] = fields[name]
		}
		if d.name == "music_control" {
			props["action"] = map[string]any{"type": "string", "enum": []string{"pause", "resume", "stop", "next", "previous", "seek"}}
		}
		if d.name != "status" {
			props["intent"] = fields["intent"]
		}
		required := d.required
		if required == nil {
			required = []string{}
		}
		tools = append(tools, map[string]any{"type": "function", "name": d.name, "description": d.description, "inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": props, "required": required}})
	}
	existing, _ := params["dynamicTools"].([]map[string]any)
	params["dynamicTools"] = append(existing, map[string]any{"type": "namespace", "name": "bridge_media", "description": "Same-call native Music and official visible YouTube session control. Native coordinator owns source effects; original bound endpoint and receipts only. Never extracts audio or changes grants.", "tools": tools})
}
func (c *Codex) handleVoiceMediaTool(id any, method string, raw json.RawMessage) bool {
	if method != "item/tool/call" {
		return false
	}
	var call struct {
		ThreadID, Namespace, Tool, CallID, TurnID string
		Arguments                                 json.RawMessage
	}
	if json.Unmarshal(raw, &call) != nil || call.Namespace != "bridge_media" {
		return false
	}
	p, ok := c.controlProvider.(backend.VoiceMediaProvider)
	if !ok {
		return false
	}
	s := c.sessionForCodexParams(codexParams(raw))
	go func() {
		caller, err := c.sessionControlCaller(s, call.TurnID, call.CallID)
		var result any
		if err == nil {
			if len(call.Arguments) > 4096 {
				err = errors.New("voice_media_arguments_invalid")
			} else {
				var input backend.VoiceMediaRequest
				d := json.NewDecoder(bytes.NewReader(call.Arguments))
				d.DisallowUnknownFields()
				err = d.Decode(&input)
				if err == nil && d.Decode(&struct{}{}) != io.EOF {
					err = errors.New("voice_media_arguments_invalid")
				}
				if err == nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if unified, ok := p.(backend.UnifiedVoiceMediaProvider); ok && unified.UnifiedVoiceMediaEnabled() {
						intent := input.IntentID
						input.IntentID = ""
						if intent == "" {
							intent = fmt.Sprintf("vop_native_%x", sha256.Sum256([]byte(call.ThreadID+"|"+call.CallID)))
						}
						var origin backend.MediaOrigin
						for {
							origin, err = c.mediaOriginForTurn(call.ThreadID, call.TurnID)
							if err == nil || ctx.Err() != nil {
								break
							}
							select {
							case <-ctx.Done():
							case <-time.After(25 * time.Millisecond):
							}
						}
						if err == nil {
							result, err = unified.ExecuteScopedVoiceMedia(ctx, caller, origin, intent, call.Tool, input)
						}
					} else {
						result, err = p.ExecuteVoiceMedia(ctx, caller, call.Tool, input)
					}
					cancel()
				}
			}
		}
		output := ""
		if err != nil {
			output = err.Error()
		} else {
			value, _ := json.Marshal(result)
			output = string(value)
		}
		if s != nil {
			c.tools.Start(s.ID, caller.RequestID, call.CallID, "bridge_media__"+call.Tool, "[voice media arguments omitted]")
			c.tools.Result(s.ID, caller.RequestID, call.CallID, output)
			c.tools.End(s.ID, caller.RequestID, call.CallID)
		}
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": err == nil, "contentItems": []map[string]any{{"type": "inputText", "text": output}}}})
	}()
	return true
}
