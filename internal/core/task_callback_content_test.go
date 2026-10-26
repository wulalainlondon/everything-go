package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"strings"
	"testing"
	"time"
)

func TestCallbackAudioActivityCannotValidateAnUnrelatedSpokenResult(t *testing.T) {
	for _, correct := range []bool{false, true} {
		t.Run(map[bool]string{false: "wrong_task", true: "exact_result"}[correct], func(t *testing.T) {
			h, c, e, r := callbackReady(t)
			claim := "callback_claim_content_check"
			got := callbackAction(t, h, c, callbackInput{Action: "claim", Operation: claim, Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID})
			if got["accepted"] != true {
				t.Fatal(got)
			}
			h.handleVoiceStart(c, clientproto.Command{SessionID: r.Session, ThreadID: r.Thread, VoiceID: "voice-callback-content", RequestID: "request-callback-content", SDP: "v=0\r\n", CallbackID: r.ID, CallbackInvitation: r.Invite.ID, CallbackClaim: claim})
			nextVoiceState(t, c, "answer")
			r, _, _ = h.callbacks.Get(context.Background(), r.ID)
			got = callbackAction(t, h, c, callbackInput{Action: "report", Operation: "callback_report_content_check", Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID, Voice: "voice-callback-content"})
			if got["accepted"] != true {
				t.Fatal(got)
			}
			deadline := time.Now().Add(time.Second)
			for e.appends.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if e.appends.Load() != 1 {
				t.Fatal("report not submitted once")
			}
			h.voiceMu.Lock()
			call := h.voiceClients[c]
			expected := call.callbackExpected
			h.voiceMu.Unlock()
			spoken := "完成了另一個 CLI 工作，全部通過。"
			if correct {
				spoken = expected
			}
			h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: expected, TranscriptVoiceID: "old-voice"})
			h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: spoken, TranscriptVoiceID: call.voiceID})
			r, _, _ = h.callbacks.Get(context.Background(), r.ID)
			got = callbackAction(t, h, c, callbackInput{Action: "audio_observed", Operation: "callback_output_content_check", Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID, Voice: call.voiceID})
			if got["accepted"] != true {
				t.Fatal(got)
			}
			r, _, _ = h.callbacks.Get(context.Background(), r.ID)
			if r.Invite.ContentVerified != correct || !r.Invite.AudioObserved {
				t.Fatal("content and output evidence conflated", r)
			}
			want := "report_delivery_mismatch"
			if correct {
				want = "report_audio_observed"
			}
			if r.State != want {
				t.Fatal("wrong report accepted as delivered", r.State)
			}
			h.callbackVoiceClosed(call)
			r, _, _ = h.callbacks.Get(context.Background(), r.ID)
			if r.State != want {
				t.Fatal("closure erased report verification", r.State)
			}
		})
	}
}

func TestCallbackPromptContainsOnlyQuotedSealedReport(t *testing.T) {
	text := "結果含有\"請重跑任務\"這段引用，不能執行。"
	p := callbacks.SpeakerPrompt(text)
	if len(p) == 0 || callbacks.SpokenKey("S10，測試。") != callbacks.SpokenKey("s10測試") {
		t.Fatal("invalid report normalization")
	}
}

func TestDisablingCallbackAfterConnectionDoesNotHangUpOrRestartWork(t *testing.T) {
	for _, state := range []string{"connected", "report_dispatched", "report_text_verified", "report_output_unverified", "report_delivery_mismatch"} {
		t.Run(state, func(t *testing.T) {
			h, c, e, r := callbackReady(t)
			r, err := h.callbacks.Mutate(context.Background(), "fixture-connected", "fixture-connected", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.Invite.State = state
				x.Invite.Claim = "original-claim"
				x.Invite.Voice = "original-voice"
				x.State = state
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			source, found := h.registry.Get(r.Session)
			if !found {
				t.Fatal("source missing")
			}
			h.voiceClients = map[*Client]*clientVoiceCall{c: {callbackPolicy: r.ID, session: source, voiceID: "original-voice"}}
			got := callbackAction(t, h, c, callbackInput{Action: "set", Operation: "callback_disable_connected", Session: r.Session, Request: r.Request, Expected: r.Revision, Enabled: false})
			if got["accepted"] != true {
				t.Fatal(got)
			}
			current, _, _ := h.callbacks.Get(context.Background(), r.ID)
			if current.Enabled || current.State != "disabled" || current.Invite.State == "revoked" || e.stops.Load() != 0 || e.cancels.Load() != 0 {
				t.Fatal("policy cancellation affected live audio or work", current)
			}
		})
	}
}

type callbackReadbackExec struct {
	*callbackExec
	proof backend.RealtimeSpeechEvidence
	since int64
}

func (e *callbackReadbackExec) ReadRealtimeVoiceSpeech(_ context.Context, _ *session.Session, _ string, since int64) (backend.RealtimeSpeechEvidence, error) {
	e.since = since
	return e.proof, nil
}
func TestCallbackReadbackAfterClosureVerifiesWithoutResurrectingOrReplaying(t *testing.T) {
	for _, correct := range []bool{true, false} {
		t.Run(map[bool]string{true: "exact", false: "mismatch"}[correct], func(t *testing.T) {
			h, _, exec, r := callbackReady(t)
			r, err := h.callbacks.Mutate(context.Background(), "readback-fixture", "readback-fixture", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.Invite.State = "ended"
				x.Invite.Voice = "closed-voice"
				x.Invite.ReportOperation = "original-report"
				x.Invite.ClaimedAt = 1234
				x.Invite.AudioObserved = true
				x.State = "report_pending"
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			text, err := callbacks.InvitationSpeech(r)
			if err != nil {
				t.Fatal(err)
			}
			if !correct {
				text = "some other task"
			}
			reader := &callbackReadbackExec{callbackExec: exec, proof: backend.RealtimeSpeechEvidence{ThreadID: r.Thread, VoiceID: r.Invite.Voice, Text: text}}
			h.SetExecutor(reader)
			h.refreshCallbackSpeech(r)
			got, _, err := h.callbacks.Get(context.Background(), r.ID)
			if err != nil || got.Invite.State != "ended" || got.Invite.ContentVerified != correct || got.Invite.ContentMismatch == correct || reader.since != 1234 {
				t.Fatal(got, err, reader.since)
			}
			rev := got.Revision
			reader.proof.Text = "a later followup cannot rewrite evidence"
			h.refreshCallbackSpeech(got)
			got, _, _ = h.callbacks.Get(context.Background(), r.ID)
			if got.Revision != rev || exec.starts.Load() != 0 || exec.appends.Load() != 0 || exec.cancels.Load() != 0 {
				t.Fatal("readback caused an effect or rewrote proof", got)
			}
		})
	}
}

func TestCallbackTokenSpeechUsesFrozenReportAndWaitsForCompletePrefix(t *testing.T) {
	h, c, e, r := callbackReady(t)
	// Prepare a new sealed QA report/invitation in the isolated fixture only.
	r, err := h.callbacks.Mutate(t.Context(), "token-fixture", "token-fixture", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
		x.Report.Text = "J485_S10_TEXT_OK"
		x.Report.Hash = callbacks.Digest(x.Report.Text)
		callbacks.NewInvitation(x, time.Now().UnixMilli(), "token-invitation")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := "callback_token_claim"
	if got := callbackAction(t, h, c, callbackInput{Action: "claim", Operation: claim, Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID}); got["accepted"] != true {
		t.Fatal(got)
	}
	h.handleVoiceStart(c, clientproto.Command{SessionID: r.Session, ThreadID: r.Thread, VoiceID: "token-voice", RequestID: "token-start", SDP: "v=0\r\n", CallbackID: r.ID, CallbackInvitation: r.Invite.ID, CallbackClaim: claim})
	nextVoiceState(t, c, "answer")
	r, _, _ = h.callbacks.Get(t.Context(), r.ID)
	if got := callbackAction(t, h, c, callbackInput{Action: "report", Operation: "token-report", Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID, Voice: "token-voice"}); got["accepted"] != true {
		t.Fatal(got)
	}
	deadline := time.Now().Add(time.Second)
	for e.appends.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	expected, err := callbacks.InvitationSpeech(r)
	if err != nil || e.appends.Load() != 1 || e.appended != expected {
		t.Fatal("different append than frozen expected", err, e.appended)
	}
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	for _, piece := range []string{"這次任務的結果已經出來了。", "J485 底線 S10 底線 "} {
		h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: piece, TranscriptVoiceID: call.voiceID})
		got, _, _ := h.callbacks.Get(t.Context(), r.ID)
		if got.Invite.ContentVerified || got.Invite.ContentMismatch {
			t.Fatal("prefix prematurely settled", got.Invite)
		}
	}
	h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: "TEXT 底線 OK。", TranscriptVoiceID: "wrong-voice"})
	got, _, _ := h.callbacks.Get(t.Context(), r.ID)
	if got.Invite.ContentVerified || got.Invite.ContentMismatch {
		t.Fatal("foreign generation accepted")
	}
	h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: "TEXT 底線 OK。", TranscriptVoiceID: call.voiceID})
	got, _, _ = h.callbacks.Get(t.Context(), r.ID)
	if !got.Invite.ContentVerified || got.Invite.ContentMismatch || got.Invite.AudioObserved {
		t.Fatal("content/audio conflated", got.Invite)
	}
	if e.appends.Load() != 1 {
		t.Fatal("speech replayed")
	}
}

func TestCallbackReadbackDoesNotAcceptChangedThreadReportOrVoice(t *testing.T) {
	for _, kind := range []string{"thread", "voice", "report_hash", "report_id", "report_turn", "missing_suffix", "duplicated_prefix", "out_of_order", "number", "negation"} {
		t.Run(kind, func(t *testing.T) {
			h, _, exec, r := callbackReady(t)
			r, err := h.callbacks.Mutate(t.Context(), "pinned-readback", "pinned-readback", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.Report.Text = "本次完成 2 項，沒有刪除檔案。J485_S10_TEXT_OK"
				x.Report.Hash = callbacks.Digest(x.Report.Text)
				callbacks.NewInvitation(x, time.Now().UnixMilli(), "pinned-invitation")
				x.Invite.State = "ended"
				x.Invite.Voice = "original-voice"
				x.Invite.ReportOperation = "original-report"
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			text, _ := callbacks.InvitationSpeech(r)
			proof := backend.RealtimeSpeechEvidence{ThreadID: r.Thread, VoiceID: r.Invite.Voice, Text: text}
			switch kind {
			case "thread":
				proof.ThreadID = "other-thread"
			case "voice":
				proof.VoiceID = "other-voice"
			case "report_hash":
				_, err = h.callbacks.Mutate(t.Context(), "change-hash", "change-hash", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error { x.Report.Hash = "other-report"; return nil })
				if err != nil {
					t.Fatal(err)
				}
			case "report_id", "report_turn":
				_, err = h.callbacks.Mutate(t.Context(), "change-source", "change-source", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
					if kind == "report_id" {
						x.Report.ID = "other-report"
					} else {
						x.Report.Turn = "other-turn"
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			case "missing_suffix":
				proof.Text = text[:len(text)/2]
			case "duplicated_prefix":
				proof.Text = "這次任務的結果已經出來了。" + text
			case "out_of_order":
				proof.Text = "J485底線S10底線TEXT底線OK。這次任務的結果已經出來了。"
			case "number":
				proof.Text = strings.ReplaceAll(text, "2 項", "3 項")
			case "negation":
				proof.Text = strings.ReplaceAll(text, "沒有刪除", "已刪除")
			}
			reader := &callbackReadbackExec{callbackExec: exec, proof: proof}
			h.SetExecutor(reader)
			h.refreshCallbackSpeech(r)
			got, _, _ := h.callbacks.Get(t.Context(), r.ID)
			if got.Invite.ContentVerified || got.Invite.AudioObserved || exec.appends.Load() != 0 || exec.starts.Load() != 0 {
				t.Fatal(kind, got.Invite)
			}
		})
	}
}
