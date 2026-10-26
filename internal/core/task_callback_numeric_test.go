package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"os"
	"strings"
	"testing"
	"time"
)

func TestActual186NumericTranscriptReadbackMatchesExactFrozenReport(t *testing.T) {
	var fixture struct {
		Expected string `json:"expected"`
		Actual   string `json:"actual"`
	}
	raw, err := os.ReadFile("testdata/callback186-numeric-transcript.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	h, _, exec, r := callbackReady(t)
	r, err = h.callbacks.Mutate(t.Context(), "numeric-actual-fixture", "numeric-actual-fixture", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
		x.Report.Text = "J486_S10_CAMERA_OK 照片主要可見綠色瓶蓋的透明瓶子、前方模糊的藍色物件，以及白色紋理牆面。"
		x.Report.Hash = callbacks.Digest(x.Report.Text)
		callbacks.NewInvitation(x, time.Now().UnixMilli(), "numeric-actual-invitation")
		x.Invite.State = "ended"
		x.Invite.Voice = "original-fixture-voice"
		x.Invite.ReportOperation = "original-fixture-report"
		x.State = "report_pending"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := callbacks.InvitationSpeech(r)
	if err != nil || expected != fixture.Expected {
		t.Fatal("Actual frozen source changed", err)
	}
	if callbacks.SpokenKey(fixture.Expected) == callbacks.SpokenKey(fixture.Actual) {
		t.Fatal("Fixture no longer reproduces original strict comparison mismatch")
	}
	reader := &callbackReadbackExec{callbackExec: exec, proof: backend.RealtimeSpeechEvidence{ThreadID: r.Thread, VoiceID: r.Invite.Voice, Text: fixture.Actual}}
	h.SetExecutor(reader)
	h.refreshCallbackSpeech(r)
	got, _, err := h.callbacks.Get(t.Context(), r.ID)
	if err != nil || !got.Invite.ContentVerified || got.Invite.ContentMismatch {
		t.Fatalf("Actual same-value numeric reading rejected: verified=%v mismatch=%v err=%v", got.Invite.ContentVerified, got.Invite.ContentMismatch, err)
	}
	if got.Invite.State != "ended" || got.Invite.AudioObserved || exec.appends.Load() != 0 || exec.starts.Load() != 0 {
		t.Fatal("Readback changed audio/closed lifecycle or caused replay")
	}
	if !strings.Contains(fixture.Actual, "J四八六底線 S十") {
		t.Fatal("not actual native evidence")
	}
}

func TestNumericSpeechLiveChunksAndOriginalInvitationProof(t *testing.T) {
	for _, kind := range []string{"exact", "wrong_digit", "foreign_voice", "changed_report", "changed_version"} {
		t.Run(kind, func(t *testing.T) {
			h, _, exec, r := callbackReady(t)
			r, err := h.callbacks.Mutate(t.Context(), "numeric-live-fixture", "numeric-live-fixture", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.Report.Text = "J486_S10_CAMERA_OK 沒有刪除檔案。"
				x.Report.Hash = callbacks.Digest(x.Report.Text)
				callbacks.NewInvitation(x, time.Now().UnixMilli(), "numeric-live-invite")
				x.Invite.Voice = "numeric-live-voice"
				x.Invite.ReportOperation = "original-numeric-report"
				x.Invite.State = "connected"
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			expected, err := callbacks.InvitationSpeech(r)
			if err != nil {
				t.Fatal(err)
			}
			source, _ := h.registry.Get(r.Session)
			call := &clientVoiceCall{session: source, voiceID: r.Invite.Voice, callbackPolicy: r.ID, callbackReport: r.Invite.ID, callbackExpected: expected, callbackSpeechVersion: 2, callbackReporting: true}
			chunks := []string{"這次任務的結果已經出來了。J四", "八六底線S十底線", "CAMERA底線OK沒有刪除檔案。"}
			if kind == "wrong_digit" {
				chunks[1] = "八七底線S十底線"
			}
			if kind == "changed_report" || kind == "changed_version" {
				_, err = h.callbacks.Mutate(t.Context(), "numeric-change-source", "numeric-change-source", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
					if kind == "changed_report" {
						x.Report.Turn = "foreign-turn"
					} else {
						x.Invite.SpeechVersion = 0
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			for i, chunk := range chunks {
				voice := call.voiceID
				if kind == "foreign_voice" {
					voice = "other-voice"
				}
				h.observeCallbackSpeech(call, backend.RealtimeVoiceEvent{State: "transcript_done", Role: "assistant", Text: chunk, TranscriptVoiceID: voice})
				got, _, _ := h.callbacks.Get(t.Context(), r.ID)
				if kind == "exact" && i < len(chunks)-1 && (got.Invite.ContentVerified || got.Invite.ContentMismatch) {
					t.Fatal("numeric prefix prematurely settled")
				}
			}
			got, _, _ := h.callbacks.Get(t.Context(), r.ID)
			if got.Invite.ContentVerified != (kind == "exact") || got.Invite.AudioObserved || exec.appends.Load() != 0 || exec.starts.Load() != 0 {
				t.Fatal("Content/source/audio/replay boundary failed", kind, got.Invite)
			}
		})
	}
}
