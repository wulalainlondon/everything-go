package jarvisrole

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
)

func TestRoleSingleCommitCASIdempotencyRecoveryAndNativeEpoch(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir, "authority")
	if e != nil {
		t.Fatal(e)
	}
	a := &Assignment{Partner: "partner", Target: Target{"authority", "session", "", 0}}
	o, e := s.Apply("original-human", "phone", 7, "r_original", "assign", "hash", 0, a)
	if e != nil || o.Revision != 1 || o.Phase != "committed" {
		t.Fatal(o, e)
	}
	if _, e = s.Apply("original-human", "phone", 7, "r_original", "assign", "different", 0, a); e == nil {
		t.Fatal("replayed different intent")
	}
	if _, e = s.Apply("other-human", "other", 9, "r_original", "assign", "hash", 0, a); e == nil {
		t.Fatal("operation stolen")
	}
	again, e := s.Apply("original-human", "phone", 7, "r_original", "assign", "hash", 0, a)
	if e != nil || again != o {
		t.Fatal(again, e)
	}
	if e = s.MigrateWitness("session", "", "native-original", 0); e != nil {
		t.Fatal(e)
	}
	rev, current := s.Snapshot()
	if rev != 2 || current.Epoch != 1 || current.Target.Thread != "native-original" {
		t.Fatal(rev, current)
	}
	s, e = Open(dir, "authority")
	if e != nil {
		t.Fatal(e)
	}
	_, current = s.Snapshot()
	if current.Device != "phone" || current.Generation != 7 || current.Actor != "original-human" {
		t.Fatal(current)
	}
	raw, _ := json.Marshal(current)
	var wire map[string]any
	json.Unmarshal(raw, &wire)
	if wire["device"] != nil || wire["credential_generation"] != nil || wire["actor"] != nil {
		t.Fatal("private witness leaked", string(raw))
	}
	if e = s.MigrateWitness("session", "native-original", "", 1); e != nil {
		t.Fatal(e)
	}
	rev, current = s.Snapshot()
	if current.Epoch != rev || current.Target.Thread != "" {
		t.Fatal(current)
	}
	if e = s.MigrateWitness("session", "stale", "wrong", 2); e == nil {
		t.Fatal("stale generation accepted")
	}
	if _, e = s.Apply("original-human", "phone", 7, "r_revoke", "revoke", "hash2", rev, nil); e != nil {
		t.Fatal(e)
	}
	_, current = s.Snapshot()
	if current != nil {
		t.Fatal("role remained")
	}
	if o, ok := s.Lookup("original-human", "r_original"); !ok || o.Revision != 1 {
		t.Fatal(o, ok)
	}
	if _, ok := s.Lookup("foreign", "r_original"); ok {
		t.Fatal("foreign receipt")
	}
	if info, e := os.Stat(s.path); e != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, e)
	}
}
func TestRoleConcurrentDevicesOneWinnerAndAuthorityIsolation(t *testing.T) {
	s, _ := Open(t.TempDir(), "authority")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, actor := range []string{"a", "b"} {
		wg.Add(1)
		go func(actor string) {
			defer wg.Done()
			_, e := s.Apply(actor, actor, 1, "r_"+actor, "assign", actor, 0, &Assignment{Partner: actor, Target: Target{Authority: "authority", Session: actor}})
			results <- e
		}(actor)
	}
	wg.Wait()
	close(results)
	wins := 0
	for e := range results {
		if e == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatal(wins)
	}
	rev, _ := s.Snapshot()
	if rev != 1 {
		t.Fatal(rev)
	}
	if _, e := Open(t.TempDir(), ""); e == nil {
		t.Fatal("empty authority accepted")
	}
	if _, e := Open(s.path[:len(s.path)-len("/jarvis-role-v1.json")], "other-authority"); e == nil {
		t.Fatal("foreign authority adopted")
	}
}

func TestAudioLeaseUnknownSurvivesRestartBlocksRoleAndOnlyOriginalActorReleases(t *testing.T) {
	root := t.TempDir()
	s, _ := Open(root, "authority")
	target := Target{"authority", "session", "", 0}
	lease := AudioLease{ID: "r_original_audio", SpeechID: "speech-original", Target: target, Epoch: 0, Phase: "acquired"}
	v, e := s.SetAudioLease("human", lease, 0, "acquire")
	if e != nil {
		t.Fatal(e)
	}
	lease.Phase = "release_unconfirmed"
	v, e = s.SetAudioLease("human", lease, v.Revision, "unknown")
	if e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, "authority")
	if e != nil || !s.AudioBusy("session") {
		t.Fatal(e)
	}
	if _, e = s.Apply("human", "phone", 1, "r_role", "assign", "assign", 0, &Assignment{Partner: "p", Target: target}); e == nil {
		t.Fatal("unknown release expired into idle")
	}
	lease.Phase = "released"
	if _, e = s.SetAudioLease("other-human", lease, v.Revision, "foreign-release"); e == nil {
		t.Fatal("another actor released mic")
	}
	if _, e = s.SetAudioLease("human", lease, v.Revision, "release"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Apply("human", "phone", 1, "r_role", "assign", "assign", 0, &Assignment{Partner: "p", Target: target}); e != nil {
		t.Fatal(e)
	}
}
