package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

type FileLease struct {
	ModifiedAt         int64  `json:"modified_at_ns"`
	ManifestIdentity   string `json:"manifest_identity"`
	ManifestModifiedAt int64  `json:"manifest_modified_at_ns"`
	ManifestBytes      int64  `json:"manifest_bytes"`
	Session            string `json:"session_id"`
	Upload             string `json:"upload_id"`
	Path               string `json:"path"`
	Bytes              int64  `json:"bytes"`
	SHA256             string `json:"sha256"`
	ManifestSHA256     string `json:"manifest_sha256"`
	Identity           string `json:"identity"`
}

func fileIdentity(f os.FileInfo) (string, error) {
	v := reflect.ValueOf(f.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", ErrGuard
	}
	dev, ino := v.FieldByName("Dev"), v.FieldByName("Ino")
	if !dev.IsValid() || !ino.IsValid() {
		return "", errors.New("file identity unavailable")
	}
	return fmt.Sprintf("%v:%v", dev.Interface(), ino.Interface()), nil
}

// CaptureFileLease is called once, before BeginRecovery's transaction. The
// frozen original hash/identity is then owned by the SAME queue database.
func CaptureFileLease(session, upload, path string, size int64) (FileLease, error) {
	l := FileLease{Session: session, Upload: upload, Path: path, Bytes: size}
	if session == "" || upload == "" || size <= 0 || size > 2<<30 || !filepath.IsAbs(path) {
		return l, ErrGuard
	}
	for p := path; ; p = filepath.Dir(p) {
		st, e := os.Lstat(p)
		if e != nil || st.Mode()&os.ModeSymlink != 0 {
			return l, ErrGuard
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	manifestPath := filepath.Join(filepath.Dir(path), "manifest.json")
	ms, e := os.Lstat(manifestPath)
	if e != nil || !ms.Mode().IsRegular() || ms.Size() > 65536 {
		return l, ErrGuard
	}
	l.ManifestIdentity, e = fileIdentity(ms)
	if e != nil {
		return l, e
	}
	l.ManifestModifiedAt = ms.ModTime().UnixNano()
	l.ManifestBytes = ms.Size()
	raw, e := os.ReadFile(manifestPath)
	if e != nil {
		return l, e
	}
	var m struct {
		Path    string `json:"path"`
		Session string `json:"session_id"`
		Upload  string `json:"upload_id"`
		SHA     string `json:"sha256"`
		Size    int64  `json:"size_bytes"`
	}
	if json.Unmarshal(raw, &m) != nil || m.Session != session || m.Upload != upload || m.Path != path || m.Size != size || len(m.SHA) != 64 {
		return l, ErrGuard
	}
	st, e := os.Lstat(path)
	if e != nil || !st.Mode().IsRegular() || st.Size() != size {
		return l, ErrGuard
	}
	l.ModifiedAt = st.ModTime().UnixNano()
	l.Identity, e = fileIdentity(st)
	if e != nil {
		return l, e
	}
	f, e := os.Open(path)
	if e != nil {
		return l, e
	}
	defer f.Close()
	actual, e := f.Stat()
	if e != nil || !os.SameFile(st, actual) {
		return l, ErrGuard
	}
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, size+1))
	if e != nil || n != size {
		return l, ErrGuard
	}
	l.SHA256 = hex.EncodeToString(h.Sum(nil))
	l.ManifestSHA256 = Hash(raw)
	if l.SHA256 != m.SHA {
		return l, ErrGuard
	}
	after, e := os.Lstat(path)
	if e != nil || !os.SameFile(st, after) || st.ModTime() != after.ModTime() || st.Size() != after.Size() {
		return l, ErrGuard
	}
	again, e := os.ReadFile(manifestPath)
	if e != nil || Hash(again) != l.ManifestSHA256 {
		return l, ErrGuard
	}
	return l, nil
}
func VerifyFileLease(original FileLease) error {
	current, e := CaptureFileLease(original.Session, original.Upload, original.Path, original.Bytes)
	if e != nil {
		return e
	}
	if current != original {
		return ErrGuard
	}
	return nil
}

func VerifyFileMetadata(l FileLease) error {
	for _, check := range []struct {
		path, identity string
		size, modified int64
	}{{l.Path, l.Identity, l.Bytes, l.ModifiedAt}, {filepath.Join(filepath.Dir(l.Path), "manifest.json"), l.ManifestIdentity, l.ManifestBytes, l.ManifestModifiedAt}} {
		st, e := os.Lstat(check.path)
		if e != nil || !st.Mode().IsRegular() || st.Size() != check.size || st.ModTime().UnixNano() != check.modified {
			return ErrGuard
		}
		identity, e := fileIdentity(st)
		if e != nil || identity != check.identity {
			return ErrGuard
		}
	}
	return nil
}
