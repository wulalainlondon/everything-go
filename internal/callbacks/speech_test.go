package callbacks

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSpeechIsBoundedSealedPresentationWithoutInstructionsOrURLs(t *testing.T) {
	out := Speech("task", "review_ready", "# 完成\n已修復 [報告](https://example.invalid/report)\n```sh\nrm -rf example\n```\n"+strings.Repeat("結果", 2000))
	if !strings.Contains(out, "等你驗收") || strings.Contains(out, "https://") || strings.Contains(out, "rm -rf") || len([]rune(out)) > 1500 {
		t.Fatal(out)
	}
}

func TestSpokenVerificationDoesNotEraseNumericMeaning(t *testing.T) {
	for _, pair := range [][2]string{{"1.5", "15"}, {"-2", "2"}, {"10%", "10"}, {"1/2", "12"}, {"$20", "20"}} {
		if SpokenKey(pair[0]) == SpokenKey(pair[1]) {
			t.Fatal("numeric meaning erased", pair)
		}
	}
	if SpokenKey("S10，測試。") != SpokenKey("s10測試") {
		t.Fatal("sentence formatting changed content")
	}
}

func TestOriginal485TokenMismatchIsNotSilentlyNormalizedAway(t *testing.T) {
	legacy := Speech("Jarvis Note20 482 驗收", "completed", "J485_S10_TEXT_OK")
	actual485 := "這次任務的結果已經出來了。J485S10TEXTOK。"
	if SpokenKey(legacy) == SpokenKey(actual485) {
		t.Fatal("old actual mismatch was erased")
	}
	rendered := ReadableSpeech("Jarvis Note20 482 驗收", "completed", "J485_S10_TEXT_OK")
	if !strings.Contains(rendered, "J485 底線 S10 底線 TEXT 底線 OK") || SpokenKey(rendered) == SpokenKey(actual485) {
		t.Fatal("identifier punctuation lost")
	}
	explicit := "這次任務的結果已經出來了。J485底線S10底線TEXT底線OK。"
	if SpokenKey(rendered) != SpokenKey(explicit) {
		t.Fatal("explicit spoken punctuation did not match")
	}
	for _, wrong := range []string{"這次任務的結果已經出來了。J486底線S10底線TEXT底線OK。", "這次任務的結果已經出來了。J485底線S11底線TEXT底線OK。", "這次任務的結果已經出來了。J485底線S10底線TEXT底線NOTOK。", "這次任務的結果已經出來了。J485底線S10。"} {
		if SpokenKey(rendered) == SpokenKey(wrong) {
			t.Fatal("different token accepted", wrong)
		}
	}
}

func TestInvitationSpeechIsFrozenAndLegacyIsNotUpgraded(t *testing.T) {
	r := Record{ID: "policy", Name: "QA", Report: &Report{Hash: Digest("A_B"), Text: "A_B", Status: "completed"}}
	NewInvitation(&r, 100, "new-intent")
	want, err := InvitationSpeech(r)
	if err != nil || r.Invite.SpeechVersion != 2 || r.Invite.SpeechHash != Digest(want) || !strings.Contains(want, "A 底線 B") {
		t.Fatal(r, err)
	}
	// Persisted new presentation survives serialization and has an explicit source.
	b, _ := json.Marshal(r)
	var decoded Record
	_ = json.Unmarshal(b, &decoded)
	if got, err := InvitationSpeech(decoded); err != nil || got != want {
		t.Fatal(got, err)
	}
	for _, mutate := range []func(*Record){func(x *Record) { x.Report.Hash = "different" }, func(x *Record) { x.Report.Text = "other" },
		func(x *Record) { x.Report.Thread = "other-thread" },
		func(x *Record) { x.Report.Turn = "other-turn" },
		func(x *Record) { x.Report.ID = "other-report" }, func(x *Record) { x.Invite.SpeechText += "other" }, func(x *Record) { x.Invite.SpeechVersion = 99 }} {
		var changed Record
		_ = json.Unmarshal(b, &changed)
		mutate(&changed)
		if _, err := InvitationSpeech(changed); err == nil {
			t.Fatal("changed source accepted")
		}
	}
	decoded.Invite = &Invitation{ID: "legacy", ReportOperation: "already-submitted"}
	if got, err := InvitationSpeech(decoded); err != nil || got != Speech(decoded.Name, decoded.Report.Status, decoded.Report.Text) {
		t.Fatal("legacy speech upgraded", got, err)
	}
}
