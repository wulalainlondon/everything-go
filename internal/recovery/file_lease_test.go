package recovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestAutomaticFileLeaseRejectsSameSizeReplacementAndSelfConsistentChangedManifest(t *testing.T) {
	for _, kind := range []string{"same-size-bytes", "bytes-and-manifest", "foreign-session", "symlink", "same-bytes-new-inode"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			dir, _ = filepath.EvalSymlinks(dir)
			p := filepath.Join(dir, "video.mp4")
			m := filepath.Join(dir, "manifest.json")
			write := func(data, session string) {
				os.WriteFile(p, []byte(data), 0600)
				raw, _ := json.Marshal(map[string]any{"path": p, "session_id": session, "upload_id": "u_original", "size_bytes": 4, "sha256": Hash([]byte(data))})
				os.WriteFile(m, raw, 0600)
			}
			write("orig", "s")
			lease, e := CaptureFileLease("s", "u_original", p, 4)
			if e != nil {
				t.Fatal(e)
			}
			if e = VerifyFileLease(lease); e != nil {
				t.Fatal("original unavailable", e)
			}
			switch kind {
			case "same-size-bytes":
				os.WriteFile(p, []byte("evil"), 0600)
			case "bytes-and-manifest":
				write("evil", "s")
			case "foreign-session":
				write("orig", "foreign")
			case "symlink":
				other := filepath.Join(t.TempDir(), "other")
				os.WriteFile(other, []byte("orig"), 0600)
				os.Remove(p)
				os.Symlink(other, p)
			case "same-bytes-new-inode":
				os.Rename(p, p+".old")
				os.WriteFile(p, []byte("orig"), 0600)
			}
			if e = VerifyFileLease(lease); e == nil {
				t.Fatal("mutated immutable file lease accepted", kind)
			}
		})
	}
}
