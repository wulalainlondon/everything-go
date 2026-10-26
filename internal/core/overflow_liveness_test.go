package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type heldCloseConn struct {
	nopConn
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (c *heldCloseConn) Close(string) {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	<-c.release
}

func TestOverflowReleasesProducerBeforeSocketClose(t *testing.T) {
	conn := &heldCloseConn{started: make(chan struct{}), release: make(chan struct{})}
	defer close(conn.release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{conn: conn, send: make(chan []byte, 1), quit: make(chan struct{}), ctx: ctx, cancel: cancel}
	c.send <- []byte("queued")
	done := make(chan struct{})
	go func() { c.enqueue([]byte("overflow")); close(done) }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("overflow did not close transport")
	}
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("producer blocked on slow socket Close; broadcast/bootstrap cannot progress")
	}
	select {
	case <-c.quit:
	default:
		t.Fatal("shutdown must precede slow transport close")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("stale work must be cancelled immediately")
	}
	healthy := &Client{conn: nopConn{}, send: make(chan []byte, 1), quit: make(chan struct{})}
	healthy.enqueue([]byte("still-live"))
	select {
	case <-healthy.send:
	default:
		t.Fatal("healthy delivery did not progress")
	}
}

func TestConcurrentOverflowSchedulesOnlyOneSocketClose(t *testing.T) {
	conn := &heldCloseConn{started: make(chan struct{}), release: make(chan struct{})}
	defer close(conn.release)
	c := &Client{conn: conn, send: make(chan []byte, 1), quit: make(chan struct{})}
	c.send <- []byte("queued")
	var workers sync.WaitGroup
	for i := 0; i < 64; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); c.enqueue([]byte("overflow")) }()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent producers blocked")
	}
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("close not started")
	}
	if n := conn.calls.Load(); n != 1 {
		t.Fatalf("close calls=%d, want1", n)
	}
	// Retired clients may not accumulate late frames even when space returns.
	<-c.send
	c.enqueue([]byte("late"))
	if len(c.send) != 0 {
		t.Fatal("late frame queued after shutdown")
	}
}
