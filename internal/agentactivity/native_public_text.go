package agentactivity

import (
	"context"
	"encoding/json"
	"unicode/utf8"
)

// PublicText revalidates the actual native turn and exact latest public item.
// Text is untrusted display data; it does not become a sealed QA verdict.
func (s *NativeSource) PublicText(ctx context.Context, rpc NativePublicRPC, child, turn string) (ref NativePublicReturn, text string, err error) {
	defer func() {
		if err != nil {
			ref = NativePublicReturn{}
			text = ""
		}
	}()
	ref, err = s.PublicReturn(ctx, rpc, child, turn)
	if err != nil {
		return
	}
	raw, e := rpc.LatestTurnItem(ctx, NativeReadProof{Scope: ref.Scope, Revision: ref.SourceRevision, source: s, publicChild: child, publicTurn: turn}, child, turn)
	if e != nil {
		err = ErrNativeSource
		return
	}
	var page struct {
		Data []struct {
			TurnID string
			Item   struct{ ID, Type, Phase, Text string }
		}
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &page) != nil || len(page.Data) != 1 {
		err = ErrNativeSource
		return
	}
	v := page.Data[0]
	if v.TurnID != turn || v.Item.ID != ref.ItemID || v.Item.Type != "agentMessage" || v.Item.Phase != "final_answer" || len(v.Item.Text) > 16384 || !utf8.ValidString(v.Item.Text) {
		err = ErrNativeSource
		return
	}
	release, e := (NativeReadProof{Scope: ref.Scope, Revision: ref.SourceRevision, source: s}).Acquire()
	if e != nil {
		err = e
		return
	}
	release()
	if rpc.Scope() != ref.Scope || ctx.Err() != nil {
		err = ErrNativeChanged
		return
	}
	text = v.Item.Text
	return
}
