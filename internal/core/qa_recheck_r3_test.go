package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"strings"
	"testing"
	"time"
)

func TestQARecheckIdentityStoreLinearizesWithFinalCommit(t *testing.T) {
	var identity serializedReadIdentity
	old := &pairedReadIdentity{token: "old", deviceID: "phone"}
	identity.Store(old)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() { finished <- identity.WithCurrent(old, func() error { close(entered); <-release; return nil }) }()
	<-entered
	replacement := &pairedReadIdentity{token: "new", deviceID: "phone"}
	stored := make(chan struct{})
	go func() { identity.Store(replacement); close(stored) }()
	select {
	case <-stored:
		t.Fatal("identity changed during exact commit")
	case <-time.After(20 * time.Millisecond):
	}
	if identity.Load() != old {
		t.Fatal("original identity replaced inside fence")
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	select {
	case <-stored:
	case <-time.After(time.Second):
		t.Fatal("retirement did not resume")
	}
	called := false
	if err := identity.WithCurrent(old, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("retired identity accepted final writer")
	}
}

func TestQARecheckFutureSettingsMediaSealSurvivesOwnQueueActivation(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer func() { s.Close(); release() }()
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	config.Model = "next-model"
	future, e := s.SetFutureConfiguration(config, nil)
	if e != nil {
		t.Fatal(e)
	}
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: future.ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": "image/png", "sha256": hex.EncodeToString(sum[:]), "name": "圖片.png", "size": len(data)}})
	reserve := messagequeue.HumanOperation{ID: "reserve_future_media", Kind: "reserve", Request: "r_future_media", Content: "", IntentHash: strings.Repeat("e", 64), Assets: assets}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(out)
	}
	prepare, item, body := mediaPrepareItem(t, h, s, c, reserve.Request, data)
	id := mediaUploadItem(t, h, s, c, prepare, item, body, data)
	seal := messagequeue.HumanOperation{ID: "seal_future_media", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: body}
	cmd := humanCoreCommand(b, seal)
	cmd.MediaBatchID = prepare.MediaBatchID
	cmd.Images = []backend.ImageAttachment{{AttachmentID: id, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}
	h.handleHumanInput(c, cmd)
	receipt, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
	if e != nil || receipt.Status != "committed" {
		t.Fatal("media seal not admitted", receipt, e)
	}
	calls := make(chan qaCorrectionConsumed, 2)
	h.SetExecutor(&qaCorrectionExecutor{fakeExec: &fakeExec{sink: h}, calls: calls})
	before := s.SettingsSnapshot()
	release()
	select {
	case got := <-calls:
		if got.request != reserve.Request || got.guard != nil || len(got.images) != 1 || got.images[0].AttachmentID != id {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		entry, _, _ := h.messageQueue.GetEffective(s.ID, reserve.Request)
		after := s.SettingsSnapshot()
		t.Fatalf("own queued settings activation rejected admitted media before source%d/config%d after source%d/config%d: state=%s message=%s", before.SourceRevision, before.ConfigRevision, after.SourceRevision, after.ConfigRevision, entry.State, entry.Message)
	}
}
