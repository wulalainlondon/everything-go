package browsercontrol

import (
	"strings"
	"testing"
)

func TestBrowserInvocationCannotSelectAuthorityOrSmuggleArguments(t *testing.T) {
	for _, params := range []string{`{"device":"other"}`, `{"voice_id":"other"}`, `{"profile":"other"}`, `{"url":"a","url":"b"}`, `{} {}`, `{"text":"` + strings.Repeat("x", 8192) + `"}`, `{"deltaY":99999}`} {
		if _, e := ParseInvocation([]string{"--id", "bop_original_intent", "--method", "open", "--params", params}); e == nil {
			t.Fatal("accepted", params[:min(50, len(params))])
		}
	}
	for _, method := range []string{"eval", "cookies", "storage", "shell", "fetch"} {
		if _, e := ParseInvocation([]string{"--id", "bop_original_intent", "--method", method}); e == nil {
			t.Fatal(method)
		}
	}
}
func TestBrowserFingerprintIncludesExactBusinessPayload(t *testing.T) {
	a, e := ParseInvocation([]string{"--id", "bop_original_intent", "--method", "open", "--params", `{"url":"https://example.com"}`})
	if e != nil {
		t.Fatal(e)
	}
	b := a
	b.Params = []byte(`{"url":"https://example.org"}`)
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatal("payload substitution")
	}
}
