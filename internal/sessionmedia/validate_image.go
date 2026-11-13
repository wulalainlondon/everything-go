package sessionmedia

import (
	"bytes"
	"encoding/binary"
	"errors"
	"image"
	"image/gif"
)

// A readable header is not a complete image. Never pin an interrupted write as
// the immutable original. Bound animated-frame allocations before DecodeAll.
func validateImage(b []byte, mime string) error {
	if mime == "image/gif" {
		if !boundedGIF(b) {
			return errors.New("GIF incomplete or frame budget exceeded")
		}
		_, err := gif.DecodeAll(bytes.NewReader(b))
		return err
	}
	_, _, err := image.Decode(bytes.NewReader(b))
	return err
}
func boundedGIF(b []byte) bool { _, ok := gifFramePixels(b); return ok }
func gifFramePixels(b []byte) (int64, bool) {
	if len(b) < 13 || (string(b[:6]) != "GIF87a" && string(b[:6]) != "GIF89a") {
		return 0, false
	}
	palette := func(packed byte) int {
		if packed&128 == 0 {
			return 0
		}
		return 3 * (1 << ((packed & 7) + 1))
	}
	at := 13 + palette(b[10])
	frames := 0
	pixels := int64(0)
	blocks := func() bool {
		for at < len(b) {
			n := int(b[at])
			at++
			if n == 0 {
				return true
			}
			at += n
			if at > len(b) {
				return false
			}
		}
		return false
	}
	for at < len(b) {
		marker := b[at]
		at++
		switch marker {
		case 0x3b:
			return pixels, frames > 0
		case 0x21:
			if at >= len(b) {
				return 0, false
			}
			at++
			if !blocks() {
				return 0, false
			}
		case 0x2c:
			if at+9 > len(b) {
				return 0, false
			}
			width, height := int64(binary.LittleEndian.Uint16(b[at+4:])), int64(binary.LittleEndian.Uint16(b[at+6:]))
			pixels += width * height
			frames++
			if width == 0 || height == 0 || pixels > 64000000 || frames > 1000 {
				return 0, false
			}
			at += 9 + palette(b[at+8])
			if at >= len(b) {
				return 0, false
			}
			at++ // LZW code size
			if !blocks() {
				return 0, false
			}
		default:
			return 0, false
		}
	}
	return 0, false
}
