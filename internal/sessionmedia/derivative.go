package sessionmedia

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"golang.org/x/image/draw"
	"image"
	"image/jpeg"
	"io"
	"os"
	"path/filepath"
)

// VerifyOriginal intentionally hashes every byte. A stat/mtime cache is not
// equivalent to the original tamper guard. No whole-original allocation occurs.
func (s *Store) VerifyOriginal(hash string) (string, int, int, int64, error) {
	if len(hash) != 64 {
		return "", 0, 0, 0, errors.New("invalid asset")
	}
	if _, e := hex.DecodeString(hash); e != nil {
		return "", 0, 0, 0, e
	}
	path := filepath.Join(s.root, "blobs", hash)
	before, e := os.Lstat(path)
	if e != nil {
		return "", 0, 0, 0, e
	}
	if !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > MaxImageBytes {
		return "", 0, 0, 0, errors.New("invalid original")
	}
	f, e := os.Open(path)
	if e != nil {
		return "", 0, 0, 0, e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		return "", 0, 0, 0, errors.New("original replaced")
	}
	digest := sha256.New()
	config, kind, e := image.DecodeConfig(io.TeeReader(f, digest))
	if e == nil {
		_, e = io.Copy(digest, io.LimitReader(f, MaxImageBytes+1))
	}
	after, se := os.Lstat(path)
	if e != nil || se != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || hex.EncodeToString(digest.Sum(nil)) != hash {
		return "", 0, 0, 0, errors.New("original changed")
	}
	mime := map[string]string{"png": "image/png", "jpeg": "image/jpeg", "gif": "image/gif", "webp": "image/webp"}[kind]
	if mime == "" || config.Width <= 0 || config.Height <= 0 || int64(config.Width)*int64(config.Height) > 64000000 {
		return "", 0, 0, 0, errors.New("dimensions unsupported")
	}
	return mime, config.Width, config.Height, before.Size(), nil
}
func (s *Store) VerifiedDerivative(hash, variant string) ([]byte, string, bool, error) {
	if _, _, _, _, e := s.VerifyOriginal(hash); e != nil {
		return nil, "", false, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.open(); e != nil {
		return nil, "", false, e
	}
	var digest string
	var size int
	if e := s.db.QueryRow("SELECT digest,bytes FROM derivatives WHERE original=? AND variant=? AND version=1", hash, variant).Scan(&digest, &size); e != nil {
		return nil, "", false, nil
	}
	b, e := readRegular(derivativePath(s.root, hash, variant))
	limit := 80 << 10
	if variant == "preview" {
		limit = 2 << 20
	}
	if e != nil || len(b) != size || len(b) > limit || IDBytes(b) != digest {
		return nil, "", false, nil
	}
	return b, "image/jpeg", true, nil
}
func (s *Store) Derivative(hash string, b []byte, variant string) ([]byte, error) {
	if IDBytes(b) != hash || (variant != "thumbnail" && variant != "preview") {
		return nil, errors.New("invalid derivative source")
	}
	// Caller has already verified the original bytes; this method does not
	// substitute a path or another occurrence's authority.
	edge, limit := 320, 80<<10
	if variant == "preview" {
		edge, limit = 2048, 2<<20
	}
	_, w, h, e := ImageInfo(b)
	if e != nil {
		return nil, e
	}
	src, _, e := image.Decode(bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	if orientation := jpegOrientation(b); orientation > 1 {
		src = orientedImage{src, orientation}
	}
	w, h = src.Bounds().Dx(), src.Bounds().Dy()
	if w > edge || h > edge {
		if w >= h {
			h = max(1, h*edge/w)
			w = edge
		} else {
			w = max(1, w*edge/h)
			h = edge
		}
	}
	// 2048 square exceeds 4MP; bound the remaining square case explicitly.
	if int64(w)*int64(h) > 4000000 {
		w = w * 2000 / 2048
		h = h * 2000 / 2048
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Src, nil)
	var out bytes.Buffer
	if e = jpeg.Encode(&out, dst, &jpeg.Options{Quality: 78}); e != nil {
		return nil, e
	}
	if out.Len() > limit {
		return nil, errors.New("derivative exceeds budget")
	}
	data := out.Bytes()
	s.mu.Lock()
	defer s.mu.Unlock()
	if e = s.open(); e != nil {
		return nil, e
	}
	// Original assets already reserve 80KiB for the thumbnail. Additional screen
	// previews share the same vault quota; no original is evicted to make space.
	var originals, previews int64
	if e = s.db.QueryRow("SELECT COALESCE(sum(bytes+81920),0) FROM assets").Scan(&originals); e != nil {
		return nil, e
	}
	if e = s.db.QueryRow("SELECT COALESCE(sum(bytes),0) FROM derivatives WHERE variant='preview' AND NOT(original=? AND variant=?)", hash, variant).Scan(&previews); e != nil {
		return nil, e
	}
	free, e := s.free(s.root)
	if e != nil || free < uint64((512<<20)+len(data)) || originals+previews+int64(len(data)) > maxVaultBytes {
		return nil, errors.New("derivative storage full")
	}
	f, e := os.CreateTemp(filepath.Join(s.root, "blobs"), ".derivative-")
	if e != nil {
		return data, nil
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_ = f.Chmod(0600)
	_, e = f.Write(data)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(tmp, derivativePath(s.root, hash, variant))
	}
	if e == nil {
		_, e = s.db.Exec("INSERT OR REPLACE INTO derivatives VALUES(?,?,1,?,?)", hash, variant, IDBytes(data), len(data))
	}
	// Cache failure never grants access to unverified data nor changes original.
	return data, nil
}

func derivativePath(root, hash, variant string) string {
	if variant == "thumbnail" {
		return filepath.Join(root, "blobs", hash+".thumb.jpg")
	}
	return filepath.Join(root, "blobs", hash+"."+variant+".v1.jpg")
}
