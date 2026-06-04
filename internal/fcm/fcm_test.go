package fcm

import (
	"strings"
	"testing"
)

func TestSummarizeStripsMarkdown(t *testing.T) {
	got := summarize("Here is **bold** and `code` and a [link](http://x).")
	if strings.ContainsAny(got, "*`") {
		t.Fatalf("markdown marks not stripped: %q", got)
	}
	if !strings.Contains(got, "link") || strings.Contains(got, "http://x") {
		t.Fatalf("link text should remain, URL should go: %q", got)
	}
}

func TestSummarizeUsesLastParagraph(t *testing.T) {
	got := summarize("first paragraph\n\nsecond paragraph is the summary.")
	if !strings.HasPrefix(got, "second paragraph") {
		t.Fatalf("should summarize the last paragraph, got %q", got)
	}
}

func TestSummarizeFirstSentenceOnly(t *testing.T) {
	got := summarize("Done. More detail follows here that should be dropped.")
	if got != "Done." {
		t.Fatalf("should keep only the first sentence, got %q", got)
	}
}

func TestSummarizeCJKSentenceWithSpace(t *testing.T) {
	// The Python bridge's split requires whitespace after the punctuation
	// ((?<=[。！？!?.])\s+), so a space-separated CJK sentence splits.
	got := summarize("完成了。 後面這句應該被丟掉。")
	if got != "完成了。" {
		t.Fatalf("CJK first-sentence split (with space) failed, got %q", got)
	}
}

func TestSummarizeCJKNoSpaceMatchesPython(t *testing.T) {
	// Without trailing whitespace there is nothing for \s+ to match, so — exactly
	// like the Python bridge — the whole text is kept (no CJK sentence split).
	got := summarize("完成了。後面這句也留著。")
	if got != "完成了。後面這句也留著。" {
		t.Fatalf("CJK without space should keep full text (Python parity), got %q", got)
	}
}

func TestSummarizeCapsAt120Runes(t *testing.T) {
	long := strings.Repeat("漢", 200) // 200 CJK runes, no sentence breaks
	got := summarize(long)
	r := []rune(got)
	// 120 runes + the ellipsis.
	if len(r) != 121 || r[120] != '…' {
		t.Fatalf("expected 120 runes + ellipsis, got %d runes (last=%q)", len(r), string(r[len(r)-1]))
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if got := summarize(""); got != "" {
		t.Fatalf("empty input should summarize to empty, got %q", got)
	}
}

func TestTokenFatal(t *testing.T) {
	if !tokenFatal(404, nil) {
		t.Fatal("404 should be fatal")
	}
	if !tokenFatal(400, []byte(`{"error":{"status":"INVALID_ARGUMENT"}}`)) {
		t.Fatal("400 INVALID_ARGUMENT should be fatal")
	}
	if !tokenFatal(404, []byte("UNREGISTERED")) {
		t.Fatal("UNREGISTERED should be fatal")
	}
	if tokenFatal(503, []byte("temporary")) {
		t.Fatal("503 transient should NOT be fatal")
	}
}
