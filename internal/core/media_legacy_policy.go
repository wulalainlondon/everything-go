package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"everything-go/internal/governance"
)

// Credential snapshots contain only derived generation, never tokens. The
// original token stays inside its existing pairing store/runtime.
func (m *mediaLifecycle) captureCredentialEra(pair *governance.Pairing) {
	if pair == nil {
		return
	}
	done, e := m.begin()
	if e != nil {
		return
	}
	defer done()
	if m.data.Credentials != nil {
		return
	}
	m.data.Credentials = map[string]uint64{}
	for _, b := range pair.DeviceBindings() {
		d := sha256.Sum256([]byte(b.Token))
		m.data.Credentials[b.DeviceID] = binary.BigEndian.Uint64(d[:8]) & ((1 << 53) - 1)
	}
	if e = m.save(); e != nil {
		m.err = e
	}
}
func hashRegularFile(path string, size int64) (string, error) {
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Size() != size {
		return "", errors.New("legacy object identity unavailable")
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 {
		return "", errors.New("legacy object has another link owner")
	}
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !os.SameFile(info, st) {
		return "", errors.New("legacy object replaced")
	}
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, size+1))
	after, se := os.Lstat(path)
	if e != nil || se != nil || n != size || !os.SameFile(info, after) || after.Size() != size || after.ModTime() != info.ModTime() {
		return "", errors.New("legacy object changed")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (m *mediaLifecycle) captureLegacy() error {
	return filepath.WalkDir(m.root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "upload-state.json" && entry.Name() != "manifest.json" {
			return nil
		}
		raw, e := os.ReadFile(path)
		if e != nil || len(raw) > 65536 {
			return errors.New("legacy metadata unavailable")
		}
		var s resumableUploadState
		var v uploadedVideoManifest
		var object string
		var size int64
		if entry.Name() == "upload-state.json" {
			if json.Unmarshal(raw, &s) != nil || s.Version != 2 {
				return errors.New("legacy pending state invalid")
			}
			if s.ExpectedSize <= mediaVideoMaxBytes {
				return nil
			}
			object = s.PartPath
			size = s.ReceivedSize
		} else {
			if json.Unmarshal(raw, &v) != nil {
				return errors.New("legacy completed manifest invalid")
			}
			if v.SizeBytes <= mediaVideoMaxBytes {
				return nil
			}
			object = v.Path
			size = v.SizeBytes
		}
		rel, e := filepath.Rel(m.root, object)
		if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return errors.New("legacy object outside store")
		}
		digest, e := hashRegularFile(object, size)
		if e != nil {
			return e
		}
		r, e := filepath.Rel(m.root, path)
		if e != nil {
			return e
		}
		m.data.Legacy[r] = mediaLegacyWitness{MetadataHash: mediaHash(json.RawMessage(raw)), FileSize: size, FileHash: digest, Pending: func() *resumableUploadState {
			if entry.Name() == "upload-state.json" {
				return &s
			}
			return nil
		}()}
		return nil
	})
}
func (u *attachmentUploads) legacyContinuation(path, part string, device string, generation uint64) bool {
	m := u.client.hub.media
	if m == nil || m.root != u.root {
		return false
	}
	done := func() {}
	if !u.lifecycleHeld {
		release, e := m.begin()
		if e != nil {
			return false
		}
		done = release
	}
	defer done()
	if m.data.Credentials[device] != generation {
		return false
	}
	rel, e := filepath.Rel(m.root, path)
	if e != nil {
		return false
	}

	w, ok := m.data.Legacy[rel]
	raw, e := os.ReadFile(path)
	if e != nil {
		return false
	}
	if ok && mediaHash(json.RawMessage(raw)) == w.MetadataHash {
		digest, e := hashRegularFile(part, w.FileSize)
		return e == nil && digest == w.FileHash
	}
	// A captured old pending intent may advance through durable prefixes and
	// completion. The immutable captured descriptor and original prefix remain
	// the authority, never a newly supplied legacy flag or current mtime.
	if filepath.Base(path) == "manifest.json" {
		w, ok = m.data.Legacy[filepath.Join(filepath.Dir(rel), "upload-state.json")]
	}
	if !ok || w.Pending == nil {
		return false
	}
	initial := w.Pending
	var current resumableUploadState
	var manifest uploadedVideoManifest
	var size int64
	if filepath.Base(path) == "upload-state.json" {
		if json.Unmarshal(raw, &current) != nil {
			return false
		}
		c, i := current, *initial
		size = c.ReceivedSize
		if size < i.ReceivedSize || size > i.ExpectedSize || !jarvisSHA.MatchString(c.PrefixSHA256) {
			return false
		}
		c.ReceivedSize = i.ReceivedSize
		c.PrefixSHA256 = i.PrefixSHA256
		// Historical missing original name/kind are preserved rather than invented.
		if i.OriginalName == "" {
			c.OriginalName = ""
		}
		if i.Kind == "" && c.Kind == "video" {
			c.Kind = ""
		}
		if mediaHash(c) != mediaHash(i) {
			return false
		}
		digest, e := hashRegularFile(part, size)
		if e != nil || digest != current.PrefixSHA256 {
			return false
		}
	} else {
		if json.Unmarshal(raw, &manifest) != nil || manifest.Scope != nil || manifest.UploadID != initial.UploadID || manifest.SessionID != initial.SessionID || manifest.DeviceID != initial.DeviceID || manifest.Name != initial.Name || manifest.MediaType != initial.MediaType || manifest.SizeBytes != initial.ExpectedSize || manifest.Path != initial.FinalPath || (initial.OriginalName != "" && manifest.OriginalName != initial.OriginalName) {
			return false
		}
		size = manifest.SizeBytes
		digest, e := hashRegularFile(part, size)
		if e != nil || digest != manifest.SHA256 {
			return false
		}
	}
	f, e := os.Open(part)
	if e != nil {
		return false
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, w.FileSize))
	return e == nil && n == w.FileSize && hex.EncodeToString(h.Sum(nil)) == w.FileHash
}
