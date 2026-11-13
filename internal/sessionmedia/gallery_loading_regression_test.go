package sessionmedia

import (
	"image/color"
	"os"
	"testing"
)

func TestQADerivativeFailedManifestMustNotLeaveUnaccountedPublishedFile(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	b := testPNG(t, color.White)
	hash, e := s.Put(b)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("CREATE TRIGGER qa_deny_manifest BEFORE INSERT ON derivatives BEGIN SELECT RAISE(ABORT,'qa-synthetic-database-failure'); END"); e != nil {
		t.Fatal(e)
	}
	_, _ = s.Derivative(hash, b, "preview")
	var count int
	s.db.QueryRow("SELECT count(*) FROM derivatives WHERE original=?", hash).Scan(&count)
	_, e = os.Stat(derivativePath(s.root, hash, "preview"))
	if count == 0 && e == nil {
		t.Fatal("published preview exists but manifest/quota contains no row after failed INSERT")
	}
}
