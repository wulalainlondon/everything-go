package eventinbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestInsertDeduplicatesBySourceEventKey(t *testing.T) {
	store := openTestStore(t)
	input := Input{Source: "github", EventKey: "run-42", Kind: "ci_failed", Severity: "error", Title: "CI failed", Data: json.RawMessage(`{"run_id":42}`)}
	first, deduped, err := store.Insert(context.Background(), input)
	if err != nil || deduped || first.ID == "" || first.Sequence == 0 {
		t.Fatalf("first=%+v deduped=%v err=%v", first, deduped, err)
	}
	second, deduped, err := store.Insert(context.Background(), input)
	if err != nil || !deduped || second.ID != first.ID || second.Sequence != first.Sequence {
		t.Fatalf("second=%+v deduped=%v err=%v", second, deduped, err)
	}
}

func TestReadAndDismissArePerDeviceAndPersist(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	event, _, err := store.Insert(context.Background(), Input{Source: "monitor", EventKey: "alert-1", Kind: "alert", Severity: "warning", Title: "Needs attention"})
	if err != nil {
		t.Fatal(err)
	}
	yes := true
	if _, err := store.Mark(context.Background(), event.ID, "desktop", &yes, &yes); err != nil {
		t.Fatal(err)
	}
	desktop, _ := store.Snapshot(context.Background(), "desktop", 0)
	phone, _ := store.Snapshot(context.Background(), "phone", 0)
	if len(desktop.Items) != 0 || len(phone.Items) != 1 || phone.Items[0].Read {
		t.Fatalf("desktop=%+v phone=%+v", desktop, phone)
	}
	_ = store.Close()
	reloaded, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	phone, _ = reloaded.Snapshot(context.Background(), "phone", 0)
	if len(phone.Items) != 1 || phone.Items[0].ID != event.ID {
		t.Fatalf("restart lost phone event: %+v", phone)
	}
}

func TestInsertRejectsExpiredOrInvalidData(t *testing.T) {
	store := openTestStore(t)
	store.now = func() time.Time { return time.UnixMilli(1000) }
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "expired", Kind: "x", Severity: "info", Title: "x", ExpiresAt: 999}); err == nil {
		t.Fatal("expired event accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "bad-json", Kind: "x", Severity: "info", Title: "x", Data: json.RawMessage(`{`)}); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "array-json", Kind: "x", Severity: "info", Title: "x", Data: json.RawMessage(`[]`)}); err == nil {
		t.Fatal("non-object JSON accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "", EventKey: "missing-source", Kind: "x", Severity: "info", Title: "x"}); err == nil {
		t.Fatal("missing canonical source accepted")
	}
}
