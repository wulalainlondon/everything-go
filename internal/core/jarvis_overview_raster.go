package core

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Original browser display must not bypass first-frame decoder bounds. This
// release permits static PNG/JPEG/GIF only; animated GIF/APNG is rejected by a
// bounded container walk before any image frame is decoded or persisted.
func overviewStaticRaster(raw []byte) error {
	if bytes.HasPrefix(raw, []byte("\x89PNG\r\n\x1a\n")) {
		for i := 8; i < len(raw); {
			if i+12 > len(raw) {
				return errors.New("invalid_png")
			}
			n := int(binary.BigEndian.Uint32(raw[i : i+4]))
			if n > len(raw)-i-12 {
				return errors.New("invalid_png")
			}
			kind := string(raw[i+4 : i+8])
			if kind == "acTL" {
				return errors.New("animated_png_unsupported")
			}
			i += n + 12
			if kind == "IEND" {
				break
			}
		}
		return nil
	}
	if !bytes.HasPrefix(raw, []byte("GIF87a")) && !bytes.HasPrefix(raw, []byte("GIF89a")) {
		return nil
	}
	if len(raw) < 13 {
		return errors.New("invalid_gif")
	}
	i := 13
	if raw[10]&128 != 0 {
		i += 3 * (1 << ((raw[10] & 7) + 1))
	}
	frames := 0
	skip := func() bool {
		for i < len(raw) {
			n := int(raw[i])
			i++
			if n == 0 {
				return true
			}
			if n > len(raw)-i {
				return false
			}
			i += n
		}
		return false
	}
	for i < len(raw) {
		kind := raw[i]
		i++
		switch kind {
		case 0x3b:
			if frames != 1 || i != len(raw) {
				return errors.New("invalid_gif")
			}
			return nil
		case 0x21:
			if i >= len(raw) {
				return errors.New("invalid_gif")
			}
			i++
			if !skip() {
				return errors.New("invalid_gif")
			}
		case 0x2c:
			frames++
			if frames > 1 {
				return errors.New("animated_gif_unsupported")
			}
			if i+9 > len(raw) {
				return errors.New("invalid_gif")
			}
			w, h := int(binary.LittleEndian.Uint16(raw[i+4:i+6])), int(binary.LittleEndian.Uint16(raw[i+6:i+8]))
			if w < 1 || h < 1 || int64(w)*int64(h) > 40_000_000 {
				return errors.New("gif_frame_decode_limit")
			}
			packed := raw[i+8]
			i += 9
			if packed&128 != 0 {
				i += 3 * (1 << ((packed & 7) + 1))
			}
			if i >= len(raw) {
				return errors.New("invalid_gif")
			}
			i++
			if !skip() {
				return errors.New("invalid_gif")
			}
		default:
			return errors.New("invalid_gif")
		}
	}
	return errors.New("invalid_gif")
}
