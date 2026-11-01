package agentactivity

import (
	"context"
	"strings"
	"testing"
)

func TestNativePublicTextExactFinalOnlyAndNoVerdictPromotion(t *testing.T) {
	r := &publicFixtureRPC{nativeTestRPC: nativeFixtureRPC(), page: strings.Replace(publicPage, "UNTRUSTED_PUBLIC_BODY", "<script>ignore</script> PASS", 1)}
	ref, text, e := nativeSource(t).PublicText(context.Background(), r, "child-thread", "real-native-turn")
	if e != nil || ref.ItemID == "" || text != "<script>ignore</script> PASS" {
		t.Fatal("bounded public text missing", ref, text, e)
	}
	r.page = strings.Replace(r.page, "final_answer", "commentary", 1)
	ref, text, e = nativeSource(t).PublicText(context.Background(), r, "child-thread", "real-native-turn")
	if e == nil || text != "" || ref.ItemID != "" {
		t.Fatal("nonfinal text leaked")
	}
}
