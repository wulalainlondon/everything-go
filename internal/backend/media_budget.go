package backend

// Shared finite media policy. These are Bridge engineering limits, not claims
// about an upstream provider's published limits.
import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

const MediaModelMaxBytes int64 = 16 * 1024 * 1024
const MediaImageMaxBytes int64 = 4 * 1024 * 1024

var ErrMediaBudget = errors.New("media materialization exceeds finite provider budget")

type mediaCounter struct{ N int64 }

func (c *mediaCounter) Write(p []byte) (int, error) {
	c.N += int64(len(p))
	if c.N > MediaModelMaxBytes {
		return 0, ErrMediaBudget
	}
	return len(p), nil
}
func HasImageRefs(images []ImageAttachment) bool {
	for _, i := range images {
		if i.AttachmentID != "" {
			return true
		}
	}
	return false
}

// Count the exact common Claude-shaped logical image envelope before reading
// any image. Codex paths do not exempt their equivalent base64 image bytes.
func ValidateMediaBudget(provider string, content string, images []ImageAttachment, files []FileAttachment) error {
	if !HasImageRefs(images) {
		return nil
	}
	if provider != Codex && provider != Claude {
		return errors.New("media adapter has no reviewed finite budget")
	}
	c := &mediaCounter{}
	if e := writeMediaFrame(c, content, images, files, false); e != nil {
		return e
	}
	// Every admitted logical envelope reserves the exact native UUID property
	// (36-byte UUID plus JSON punctuation), even if an adapter omits it.
	if c.N+46 > MediaModelMaxBytes {
		return ErrMediaBudget
	}
	return nil
}
func quoted(w io.Writer, v string) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = w.Write(b)
	return e
}
func literal(w io.Writer, v string) error { _, e := io.WriteString(w, v); return e }

// WriteMediaFrame emits one bounded native Claude frame. Each image is streamed
// directly through its base64 encoder into the sole serialized output buffer.
func WriteMediaFrame(w io.Writer, content string, images []ImageAttachment, files []FileAttachment) error {
	return writeMediaFrame(w, content, images, files, true)
}
func writeMediaFrame(w io.Writer, content string, images []ImageAttachment, files []FileAttachment, materialize bool) error {
	if e := literal(w, `{"type":"user","message":{"role":"user","content":[`); e != nil {
		return e
	}
	comma := false
	add := func() error {
		if comma {
			return literal(w, ",")
		}
		comma = true
		return nil
	}
	for _, img := range images {
		if e := add(); e != nil {
			return e
		}
		mt := img.MediaType
		if mt == "" {
			mt = "image/jpeg"
		}
		if e := literal(w, `{"type":"image","source":{"type":"base64","media_type":`); e != nil {
			return e
		}
		if e := quoted(w, mt); e != nil {
			return e
		}
		if e := literal(w, `,"data":"`); e != nil {
			return e
		}
		if img.AttachmentID != "" {
			if img.Data != "" || img.VerifiedPath == "" || img.SizeBytes <= 0 || img.SizeBytes > MediaImageMaxBytes || len(img.SHA256) != 64 {
				return errors.New("invalid server image reference")
			}
			if materialize {
				encoder := base64.NewEncoder(base64.StdEncoding, w)
				if e := CopyVerifiedImage(encoder, img); e != nil {
					return e
				}
				if e := encoder.Close(); e != nil {
					return e
				}
			} else {
				n := int64(base64.StdEncoding.EncodedLen(int(img.SizeBytes)))
				if c, ok := w.(*mediaCounter); ok {
					c.N += n
					if c.N > MediaModelMaxBytes {
						return ErrMediaBudget
					}
				} else {
					return errors.New("metadata counter required")
				}
			}
		} else {
			if e := literal(w, img.Data); e != nil {
				return e
			}
		}
		if e := literal(w, `"}}`); e != nil {
			return e
		}
	}
	for _, f := range files {
		if e := add(); e != nil {
			return e
		}
		if f.MediaType == "application/pdf" {
			b, _ := json.Marshal(map[string]any{"type": "document", "source": map[string]string{"type": "base64", "media_type": "application/pdf", "data": f.Content}})
			if _, e := w.Write(b); e != nil {
				return e
			}
			continue
		}
		name := f.Name
		if name == "" {
			name = "file"
		}
		text := f.Content
		if j := strings.LastIndex(name, "."); j >= 0 && j+1 < len(name) {
			text = "```" + name[j+1:] + "\n" + text + "\n```"
		}
		if e := literal(w, `{"type":"text","text":`); e != nil {
			return e
		}
		if e := quoted(w, "[File: "+name+"]\n"+text); e != nil {
			return e
		}
		if e := literal(w, "}"); e != nil {
			return e
		}
	}
	if content != "" {
		if e := add(); e != nil {
			return e
		}
		if e := literal(w, `{"type":"text","text":`); e != nil {
			return e
		}
		if e := quoted(w, content); e != nil {
			return e
		}
		if e := literal(w, "}"); e != nil {
			return e
		}
	}
	return literal(w, "]}}\n")
}

// Never follow a path replacement or trust a client-supplied file path. Only
// core-resolved paths reach this function; reader/history pins outlive handoff.
func CopyVerifiedImage(w io.Writer, img ImageAttachment) error {
	before, e := os.Lstat(img.VerifiedPath)
	if e != nil || !before.Mode().IsRegular() || before.Size() != img.SizeBytes {
		return errors.New("image object unavailable")
	}
	if st, ok := before.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 {
		return errors.New("image object must have one owned link")
	}
	f, e := os.Open(img.VerifiedPath)
	if e != nil {
		return e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		return errors.New("image object replaced")
	}
	digest := sha256.New()
	n, e := io.Copy(io.MultiWriter(w, digest), io.LimitReader(f, img.SizeBytes+1))
	if e != nil {
		return e
	}
	after, e := os.Lstat(img.VerifiedPath)
	if e != nil || !os.SameFile(before, after) || after.Size() != before.Size() || after.ModTime() != before.ModTime() || n != img.SizeBytes || hex.EncodeToString(digest.Sum(nil)) != img.SHA256 {
		return fmt.Errorf("image object digest/identity mismatch")
	}
	return nil
}
