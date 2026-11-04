package backend

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func budgetImage(t *testing.T, data []byte) ImageAttachment {
	t.Helper()
	path := filepath.Join(t.TempDir(), "object.jpg")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(data)
	return ImageAttachment{AttachmentID: "u_" + strings.Repeat("a", 32), MediaType: "image/jpeg", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(h[:]), VerifiedPath: path}
}
func TestMediaRefsCountingAndNativeSerialization(t *testing.T) {
	img := budgetImage(t, []byte("known immutable bytes"))
	text := "中文\nquote \"\\"
	c := &mediaCounter{}
	if e := writeMediaFrame(c, text, []ImageAttachment{img}, nil, false); e != nil {
		t.Fatal(e)
	}
	var actual bytes.Buffer
	if e := WriteMediaFrame(&actual, text, []ImageAttachment{img}, nil); e != nil {
		t.Fatal(e)
	}
	if int64(actual.Len()) != c.N {
		t.Fatalf("metadata=%d actual=%d", c.N, actual.Len())
	}
	if !json.Valid(actual.Bytes()) {
		t.Fatal("invalid native frame")
	}
	for _, provider := range []string{Codex, Claude} {
		if e := ValidateMediaBudget(provider, text, []ImageAttachment{img}, nil); e != nil {
			t.Fatal(e)
		}
	}
}
func TestMediaRefsExact16MiBAndPlusOne(t *testing.T) {
	img := budgetImage(t, []byte{1, 2, 3})
	c := &mediaCounter{}
	if e := writeMediaFrame(c, "x", []ImageAttachment{img}, nil, false); e != nil {
		t.Fatal(e)
	}
	n := int(MediaModelMaxBytes-c.N-46) + 1
	text := strings.Repeat("a", n)
	if e := ValidateMediaBudget(Codex, text, []ImageAttachment{img}, nil); e != nil {
		t.Fatal(e)
	}
	if e := ValidateMediaBudget(Codex, text+"a", []ImageAttachment{img}, nil); e == nil {
		t.Fatal("16MiB+1 admitted")
	}
}
func TestMediaRefs8x4MiBRejectedBeforeRead(t *testing.T) {
	img := ImageAttachment{AttachmentID: "u_" + strings.Repeat("b", 32), MediaType: "image/jpeg", SizeBytes: MediaImageMaxBytes, SHA256: strings.Repeat("0", 64), VerifiedPath: "/does/not/exist"}
	if e := ValidateMediaBudget(Codex, "", []ImageAttachment{img, img, img, img, img, img, img, img}, nil); e != ErrMediaBudget {
		t.Fatalf("must count without reading, got%v", e)
	}
	if e := ValidateMediaBudget("unknown", "", []ImageAttachment{img}, nil); e == nil {
		t.Fatal("unknown provider finite budget must reject")
	}
}
func TestMediaRefsChangedBytesAndSymlinkRefused(t *testing.T) {
	img := budgetImage(t, []byte("original"))
	if e := os.WriteFile(img.VerifiedPath, []byte("modified"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := CopyVerifiedImage(&bytes.Buffer{}, img); e == nil {
		t.Fatal("digest replacement accepted")
	}
	path := img.VerifiedPath
	link := path + "-link"
	if e := os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	img.VerifiedPath = link
	if e := CopyVerifiedImage(&bytes.Buffer{}, img); e == nil {
		t.Fatal("symlink accepted")
	}
}
