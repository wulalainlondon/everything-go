package sessionmedia

import (
	"encoding/binary"
	"image"
	"image/color"
)

// A bounded EXIF metadata probe. The oriented view avoids a second full-size
// bitmap while resampling camera JPEGs into a display-only derivative.
func jpegOrientation(b []byte) int {
	if len(b) < 4 || b[0] != 255 || b[1] != 216 {
		return 1
	}
	for at := 2; at+4 <= len(b); {
		if b[at] != 255 {
			return 1
		}
		for at < len(b) && b[at] == 255 {
			at++
		}
		if at >= len(b) {
			return 1
		}
		marker := b[at]
		at++
		if marker == 217 || marker == 218 {
			return 1
		}
		if marker == 1 || marker >= 208 && marker <= 215 {
			continue
		}
		if at+2 > len(b) {
			return 1
		}
		n := int(binary.BigEndian.Uint16(b[at:]))
		if n < 2 || n > len(b)-at {
			return 1
		}
		if marker == 225 && n >= 16 && string(b[at+2:at+8]) == "Exif\x00\x00" {
			t := b[at+8 : at+n]
			var order binary.ByteOrder
			if string(t[:2]) == "II" {
				order = binary.LittleEndian
			} else if string(t[:2]) == "MM" {
				order = binary.BigEndian
			} else {
				return 1
			}
			if order.Uint16(t[2:]) != 42 {
				return 1
			}
			offset := uint64(order.Uint32(t[4:]))
			if offset+2 > uint64(len(t)) {
				return 1
			}
			count := uint64(order.Uint16(t[offset:]))
			for i := uint64(0); i < count; i++ {
				entry := offset + 2 + i*12
				if entry+12 > uint64(len(t)) {
					return 1
				}
				v := t[entry : entry+12]
				if order.Uint16(v) == 274 && order.Uint16(v[2:]) == 3 && order.Uint32(v[4:]) == 1 {
					o := int(order.Uint16(v[8:]))
					if o >= 1 && o <= 8 {
						return o
					}
					return 1
				}
			}
		}
		at += n
	}
	return 1
}

type orientedImage struct {
	source      image.Image
	orientation int
}

func (o orientedImage) ColorModel() color.Model { return o.source.ColorModel() }
func (o orientedImage) Bounds() image.Rectangle {
	r := o.source.Bounds()
	if o.orientation >= 5 {
		return image.Rect(0, 0, r.Dy(), r.Dx())
	}
	return image.Rect(0, 0, r.Dx(), r.Dy())
}
func (o orientedImage) At(x, y int) color.Color {
	r := o.source.Bounds()
	w, h := r.Dx(), r.Dy()
	switch o.orientation {
	case 2:
		x = w - 1 - x
	case 3:
		x, y = w-1-x, h-1-y
	case 4:
		y = h - 1 - y
	case 5:
		x, y = y, x
	case 6:
		x, y = y, h-1-x
	case 7:
		x, y = w-1-y, h-1-x
	case 8:
		x, y = w-1-y, x
	}
	return o.source.At(x+r.Min.X, y+r.Min.Y)
}
