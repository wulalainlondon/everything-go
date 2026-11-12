package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

type galleryIOWaiter struct {
	binding  string
	priority int
	at       time.Time
	ready    chan struct{}
	running  bool
}
type galleryIOAdmission struct {
	mu                 sync.Mutex
	active, background int
	queue              []*galleryIOWaiter
}

func (a *galleryIOAdmission) dispatch() {
	for a.active < 2 {
		best := -1
		for i, w := range a.queue {
			if w.priority > 0 && a.background >= 1 {
				continue
			}
			if best < 0 || galleryRank(w) < galleryRank(a.queue[best]) {
				best = i
			}
		}
		if best < 0 {
			return
		}
		w := a.queue[best]
		a.queue = append(a.queue[:best], a.queue[best+1:]...)
		w.running = true
		a.active++
		if w.priority > 0 {
			a.background++
		}
		close(w.ready)
	}
}
func galleryRank(w *galleryIOWaiter) int {
	if w.priority > 1 && time.Since(w.at) >= 2*time.Second {
		return 1
	}
	return w.priority
}
func (a *galleryIOAdmission) acquire(ctx context.Context, binding string, priority int) (func(), error) {
	w := &galleryIOWaiter{binding: binding, priority: priority, at: time.Now(), ready: make(chan struct{})}
	a.mu.Lock()
	n := 0
	for _, item := range a.queue {
		if item.binding == binding {
			n++
		}
	}
	if len(a.queue) >= 32 || n >= 8 {
		a.mu.Unlock()
		return nil, errors.New("gallery IO capacity pending")
	}
	a.queue = append(a.queue, w)
	a.dispatch()
	a.mu.Unlock()
	release := func() {
		a.mu.Lock()
		if w.running {
			w.running = false
			a.active--
			if w.priority > 0 {
				a.background--
			}
		} else {
			for i, item := range a.queue {
				if item == w {
					a.queue = append(a.queue[:i], a.queue[i+1:]...)
					break
				}
			}
		}
		a.dispatch()
		a.mu.Unlock()
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-w.ready:
		if ctx.Err() != nil {
			release()
			return nil, ctx.Err()
		}
		return release, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	case <-timer.C:
		release()
		return nil, errors.New("gallery IO deadline")
	}
}
