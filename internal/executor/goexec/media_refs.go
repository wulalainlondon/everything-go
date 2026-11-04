package goexec

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// At most two bounded serializer/stagers over all sessions/providers. Each
// request materializes images sequentially, never eight decode/base64 copies.
var mediaMaterializers = make(chan struct{}, 2)

type boundedMediaBuffer struct{ bytes.Buffer }

func (b *boundedMediaBuffer) Write(p []byte) (int, error) {
	if int64(b.Len()+len(p)) > backend.MediaModelMaxBytes {
		return 0, backend.ErrMediaBudget
	}
	return b.Buffer.Write(p)
}
func checkedUserMessageJSON(content string, images []backend.ImageAttachment, files []backend.FileAttachment) ([]byte, error) {
	if !backend.HasImageRefs(images) {
		return userMessageJSON(content, images, files), nil
	}
	if e := backend.ValidateMediaBudget(backend.Claude, content, images, files); e != nil {
		return nil, e
	}
	mediaMaterializers <- struct{}{}
	defer func() { <-mediaMaterializers }()
	b := &boundedMediaBuffer{}
	if e := backend.WriteMediaFrame(b, content, images, files); e != nil {
		return nil, e
	}
	return b.Bytes(), nil
}
func (c *Codex) prepareCodexImageRef(s *session.Session, reqID, owner string, img backend.ImageAttachment, st *codexState) (map[string]any, error) {
	root := filepath.Join(c.dataDir, "codex-input-images")
	if e := os.MkdirAll(root, 0755); e != nil {
		return nil, e
	}
	sHash := sha256.Sum256([]byte(s.ID))
	rHash := sha256.Sum256([]byte(reqID))
	path := filepath.Join(root, fmt.Sprintf("%x_%x_%s%s", sHash[:6], rHash[:6], randHex(8), codexImageExt(img.MediaType)))
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	e = backend.CopyVerifiedImage(f, img)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		os.Remove(path)
		return nil, e
	}
	st.mu.Lock()
	if st.tempImages == nil {
		st.tempImages = map[string][]string{}
	}
	st.tempImages[owner] = append(st.tempImages[owner], path)
	st.mu.Unlock()
	return map[string]any{"type": "localImage", "path": path}, nil
}
