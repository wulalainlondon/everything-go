package workspace

import "testing"

func TestUnifiedNoticeWorkspaceReadDistinguishesMissingAndStoreUnknown(t *testing.T) {
	f := setup(t)
	if _, allowed, err := f.s.PairedOwnerTaskReadState(f.ctx, f.owner, f.room.ID, "missing-task"); err != nil || allowed {
		t.Fatalf("known missing task allowed=%v err=%v", allowed, err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, allowed, err := f.s.PairedOwnerTaskReadState(f.ctx, f.owner, f.room.ID, "missing-task"); err == nil || allowed {
		t.Fatalf("closed original store must be unknown allowed=%v err=%v", allowed, err)
	}
}
