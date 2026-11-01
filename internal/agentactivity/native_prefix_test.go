package agentactivity

import (
	"errors"
	"io"
	"strings"
	"testing"
)

const nativeHeader = `{"type":"session_meta","payload":{"id":"root-thread"}}` + "\n"
const nativeRecord = `{"type":"event_msg","payload":{"type":"item_completed","thread_id":"root-thread","turn_id":"parent-turn","completed_at_ms":1000,"item":{"type":"SubAgentActivity","id":"event","kind":"started","agent_thread_id":"child-thread","agent_path":"/root/worker"}}}` + "\n"

func TestNativePrefixStrictHeaderAndRecordCategories(t *testing.T) {
	for _, header := range []string{strings.Replace(nativeHeader, `"id":"root-thread"`, `"id":"root-thread","ID":"other"`, 1), strings.Replace(nativeHeader, "root-thread", "other-root", 1), nativeHeader[:len(nativeHeader)-1], strings.Repeat(" ", MaxNativePrefixRecord) + nativeHeader, string([]byte{'{', '"', 0xff, '"', ':', '1', '}', '\n'}), strings.Replace(nativeHeader, `"id":"root-thread"`, `"id":"root-thread","extra":`+strings.Repeat("[", 33)+"0"+strings.Repeat("]", 33), 1)} {
		if _, e := ReadNativePrefix(strings.NewReader(header), "root-thread", func([]byte) error { return nil }); e == nil {
			t.Fatal("unqualified native header accepted")
		}
	}
	unrelated := `{"type":"response_item","payload":{"type":"agent_message"}}` + "\n"
	rejected := strings.Replace(nativeRecord, "child-thread", "root-thread", 1)
	accepted := 0
	s, e := ReadNativePrefix(strings.NewReader(nativeHeader+"{bad}\n"+unrelated+rejected+nativeRecord), "root-thread", func([]byte) error { accepted++; return nil })
	if e != nil || accepted != 1 || s.Records != 4 || s.Malformed != 1 || s.Unrelated != 1 || s.RejectedNative != 1 || s.NativeEvents != 1 {
		t.Fatalf("native rejection categories lost: %+v %v", s, e)
	}
}
func TestNativePrefixTailsCountEachRecordOnce(t *testing.T) {
	for _, body := range []string{`{"partial":`, strings.Repeat("x", MaxNativePrefixRecord+17), strings.Repeat("x", 2*MaxNativePrefixRecord+17), strings.Repeat("x", MaxNativePrefixRecord+17) + "\n"} {
		s, e := ReadNativePrefix(strings.NewReader(nativeHeader+body), "root-thread", func([]byte) error { t.Fatal("omitted content delivered"); return nil })
		partial := !strings.HasSuffix(body, "\n")
		over := len(body) > MaxNativePrefixRecord
		if e != nil || s.Records != 1 || (s.PartialTail == 1) != partial || (s.Oversized == 1) != over || partial && s.PartialTailBytes != uint64(len(body)) || over && s.OversizedBytes != uint64(len(body)) {
			t.Fatalf("tail counted incorrectly: %+v %v", s, e)
		}
	}
}

type nativeFaultReader struct{ err error }

func (r nativeFaultReader) Read([]byte) (int, error) { return 0, r.err }
func TestNativePrefixPreservesIOAndCallbackFailures(t *testing.T) {
	sentinel := errors.New("original source IO failure")
	if _, e := ReadNativePrefix(io.MultiReader(strings.NewReader(nativeHeader), nativeFaultReader{sentinel}), "root-thread", func([]byte) error { return nil }); !errors.Is(e, sentinel) {
		t.Fatal("source IO error swallowed", e)
	}
	if s, e := ReadNativePrefix(strings.NewReader(nativeHeader+nativeRecord), "root-thread", func([]byte) error { return sentinel }); !errors.Is(e, sentinel) || s.NativeEvents != 0 {
		t.Fatal("callback failure swallowed or counted positive", e)
	}
}
