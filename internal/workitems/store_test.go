package workitems

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.now = func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	return s
}

func seedItem(t *testing.T, s *Store, projectID, itemID string) WorkItem {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateProject(ctx, CreateProjectInput{ID: projectID, Name: projectID}); err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateItem(ctx, CreateItemInput{ID: itemID, ProjectID: projectID, Title: "Build native board", Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func move(t *testing.T, s *Store, item WorkItem, lifecycle Lifecycle, actor ActorType) WorkItem {
	t.Helper()
	item, err := s.MoveItem(context.Background(), MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: lifecycle, Actor: Actor{Type: actor}})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func TestLifecycleRequiresHumanAcceptance(t *testing.T) {
	s := openTestStore(t)
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	item = move(t, s, item, LifecycleActive, ActorAgent)
	item = move(t, s, item, LifecycleReview, ActorAgent)

	_, err := s.MoveItem(context.Background(), MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: LifecycleDone, Actor: Actor{Type: ActorAgent}})
	if !errors.Is(err, ErrHumanRequired) {
		t.Fatalf("agent done error=%v, want ErrHumanRequired", err)
	}

	item = move(t, s, item, LifecycleDone, ActorMobile)
	if item.AcceptedAt == nil || item.Lifecycle != LifecycleDone {
		t.Fatalf("human acceptance not persisted: %+v", item)
	}
	item = move(t, s, item, LifecycleActive, ActorUser)
	if item.AcceptedAt != nil {
		t.Fatal("reopened item retained accepted_at")
	}
}

func TestOptimisticConflictReturnsCurrentItem(t *testing.T) {
	s := openTestStore(t)
	item := seedItem(t, s, "p1", "i1")
	newTitle := "Updated"
	updated, err := s.UpdateItem(context.Background(), UpdateItemInput{ID: item.ID,
		ExpectedVersion: item.Version, Title: &newTitle, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateItem(context.Background(), UpdateItemInput{ID: item.ID,
		ExpectedVersion: item.Version, Title: &newTitle, Actor: Actor{Type: ActorUser}})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Current.Version != updated.Version {
		t.Fatalf("conflict=%v current=%+v", err, conflict)
	}
}

func TestOneActiveWorkItemPerSession(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	i1 := seedItem(t, s, "p1", "i1")
	i2, err := s.CreateItem(ctx, CreateItemInput{ID: "i2", ProjectID: "p1", Title: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	_, i1, err = s.LinkSession(ctx, LinkSessionInput{WorkItemID: i1.ID, SessionID: "s1",
		ExpectedVersion: i1.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.LinkSession(ctx, LinkSessionInput{WorkItemID: i2.ID, SessionID: "s1",
		ExpectedVersion: i2.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrSessionLinked) {
		t.Fatalf("link error=%v, want ErrSessionLinked", err)
	}
}

func TestDependencyCycleAndCrossProjectRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a := seedItem(t, s, "p1", "a")
	b, _ := s.CreateItem(ctx, CreateItemInput{ID: "b", ProjectID: "p1", Title: "B"})
	c, _ := s.CreateItem(ctx, CreateItemInput{ID: "c", ProjectID: "p1", Title: "C"})
	other := seedItem(t, s, "p2", "other")

	a, err := s.AddDependency(ctx, AddDependencyInput{WorkItemID: a.ID, DependsOnID: b.ID,
		ExpectedVersion: a.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: b.ID, DependsOnID: c.ID,
		ExpectedVersion: b.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: c.ID, DependsOnID: a.ID,
		ExpectedVersion: c.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("cycle error=%v", err)
	}
	_, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: a.ID, DependsOnID: other.ID,
		ExpectedVersion: a.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrCrossProject) {
		t.Fatalf("cross-project error=%v", err)
	}
}

func TestRestartPreservesSnapshotAndRevision(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "Project"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateItem(ctx, CreateItemInput{ID: "i1", ProjectID: project.ID, Title: "Persist me"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Projects) != 1 || len(snap.Items) != 1 || snap.Items[0].ID != item.ID || snap.Revision < 2 {
		t.Fatalf("restart snapshot=%+v", snap)
	}
	if _, err := filepath.Abs(filepath.Join(dir, "everything_go_work_items.db")); err != nil {
		t.Fatal(err)
	}
}

func TestRelatedEntitiesAreTransactionalAndAppearInDeltas(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	link, item, err := s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	changes, _, _, err := s.ChangesSince(ctx, item.ActivityRevision-1, 10)
	if err != nil || len(changes) != 1 || !strings.Contains(string(changes[0].Payload), `"link"`) {
		t.Fatalf("link delta=%+v err=%v", changes, err)
	}
	link, item, err = s.UnlinkSession(ctx, UnlinkSessionInput{LinkID: link.ID,
		ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil || link.UnlinkedAt == nil {
		t.Fatalf("unlink=%+v item=%+v err=%v", link, item, err)
	}

	comment, item, err := s.AddComment(ctx, AddCommentInput{ID: "comment1", WorkItemID: item.ID,
		ExpectedVersion: item.Version, Body: "  ready for review  ", Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || comment.Body != "ready for review" {
		t.Fatalf("comment=%+v err=%v", comment, err)
	}
	comment, item, err = s.EditComment(ctx, EditCommentInput{CommentID: comment.ID,
		ExpectedVersion: item.Version, Body: "approved soon", Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || comment.EditedAt == nil {
		t.Fatalf("edited comment=%+v err=%v", comment, err)
	}
	item, err = s.ArchiveItem(ctx, ArchiveItemInput{ID: item.ID, ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil || item.ArchivedAt == nil {
		t.Fatalf("archive=%+v err=%v", item, err)
	}
	item, err = s.ArchiveItem(ctx, ArchiveItemInput{ID: item.ID, ExpectedVersion: item.Version, Restore: true, Actor: Actor{Type: ActorUser}})
	if err != nil || item.ArchivedAt != nil {
		t.Fatalf("restore=%+v err=%v", item, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.SessionLinks) != 0 || len(snap.Comments) != 1 || snap.Comments[0].Body != "approved soon" {
		t.Fatalf("snapshot after related mutations=%+v", snap)
	}
}

func TestExplicitRunProjectsToReviewButOrdinaryLinkedSessionDoesNot(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, err := s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := s.AdvanceRun(ctx, "s1", "ordinary", "succeeded", "")
	if err != nil || ordinary.Changed {
		t.Fatalf("ordinary linked turn changed work: %+v err=%v", ordinary, err)
	}
	run, item, err := s.StartRun(ctx, StartRunInput{ID: "run1", WorkItemID: item.ID,
		SessionID: "s1", RequestID: "req1", Kind: "implementation", ExpectedVersion: item.Version,
		Actor: Actor{Type: ActorMobile}})
	if err != nil || run.Status != "queued" || item.Lifecycle != LifecycleActive {
		t.Fatalf("start run=%+v item=%+v err=%v", run, item, err)
	}
	update, err := s.AdvanceRun(ctx, "s1", "req1", "running", "")
	if err != nil || !update.Changed || update.Item.Lifecycle != LifecycleActive {
		t.Fatalf("running update=%+v err=%v", update, err)
	}
	update, err = s.AdvanceRun(ctx, "s1", "req1", "succeeded", "")
	if err != nil || !update.Changed || update.Item.Lifecycle != LifecycleReview || update.Attention != "review_ready" {
		t.Fatalf("success update=%+v err=%v", update, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil || len(snap.Runs) != 1 || snap.Runs[0].FinishedAt == nil {
		t.Fatalf("run snapshot=%+v err=%v", snap.Runs, err)
	}
}

func TestFailedExplicitRunCreatesAttentionWithoutCompletingWork(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, _ = s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	_, item, err := s.StartRun(ctx, StartRunInput{ID: "run1", WorkItemID: item.ID,
		SessionID: "s1", RequestID: "req1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	update, err := s.AdvanceRun(ctx, "s1", "req1", "failed", "tool failed")
	if err != nil || update.Item.Lifecycle != LifecycleActive || update.Attention != "execution_failed" ||
		update.Item.BlockedReasonCode != "execution_failed" {
		t.Fatalf("failure projection=%+v err=%v", update, err)
	}
}
