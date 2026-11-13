package sessionmedia

import (
	"errors"
	"sync"
	"time"
)

// One process-wide capture/derivative decoder. Encoded inputs remain bounded by
// the caller scan/IO limits; queued jobs allocate no additional decode buffers.
var imageDecodes = struct {
	sync.Mutex
	active  bool
	waiting int
	changed chan struct{}
}{changed: make(chan struct{})}

func acquireImageDecode(b []byte, w, h int, animated, derivative bool) (func(), error) {
	pixels := int64(w) * int64(h)
	if w <= 0 || h <= 0 || pixels > 64000000 {
		return nil, errors.New("image dimensions unavailable")
	}
	if animated {
		var ok bool
		pixels, ok = gifFramePixels(b)
		if !ok {
			return nil, errors.New("GIF incomplete or frame budget exceeded")
		}
	}
	// Covers source, validation/codec working storage, scale output and encoded
	// copies. Inputs whose conservative estimate exceeds the pool fail closed.
	cost := int64(len(b))*4 + pixels*8
	if derivative {
		cost += 4000000*8 + (2<<20)*4
	}
	if pixels > 64000000 || cost > 192<<20 {
		return nil, errors.New("image processing budget pending qualification")
	}
	imageDecodes.Lock()
	if imageDecodes.waiting >= 16 {
		imageDecodes.Unlock()
		return nil, errors.New("image decode queue full")
	}
	imageDecodes.waiting++
	imageDecodes.Unlock()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		imageDecodes.Lock()
		if !imageDecodes.active {
			imageDecodes.waiting--
			imageDecodes.active = true
			imageDecodes.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					imageDecodes.Lock()
					imageDecodes.active = false
					close(imageDecodes.changed)
					imageDecodes.changed = make(chan struct{})
					imageDecodes.Unlock()
				})
			}, nil
		}
		changed := imageDecodes.changed
		imageDecodes.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			imageDecodes.Lock()
			imageDecodes.waiting--
			imageDecodes.Unlock()
			return nil, errors.New("image decode busy")
		}
	}
}
