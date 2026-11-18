package syncer

import (
	"io"
	"sync/atomic"
	"time"
)

// progressWriter feeds the live throughput meter while a body is copied.
// It wraps the destination, not the source, so the count is what actually
// reached the file (or the socket) rather than what was merely offered.
type progressWriter struct {
	w      io.Writer
	onMove func(int64)
	stall  *stallWatch
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		p.stall.touch()
		if p.onMove != nil {
			p.onMove(int64(n))
		}
	}
	return n, err
}

// progressReader counts bytes as the transport reads them from a local file,
// which makes an upload's rate live instead of known only at completion.
type progressReader struct {
	r      io.Reader
	onMove func(int64)
	stall  *stallWatch
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.stall.touch()
		if p.onMove != nil {
			p.onMove(int64(n))
		}
	}
	return n, err
}

// stallWatch aborts a transfer that stops carrying bytes. Waiting for the full
// per-file deadline leaves a dead tunnel looking like "stuck with no speed"
// for minutes or hours; watching last-progress catches it in about three
// minutes while any real progress keeps the transfer alive.
type stallWatch struct {
	last atomic.Int64 // Unix nanos of the last byte seen
}

func newStallWatch() *stallWatch {
	w := &stallWatch{}
	w.touch()
	return w
}

func (w *stallWatch) touch() {
	if w != nil {
		w.last.Store(time.Now().UnixNano())
	}
}

func (w *stallWatch) stalledFor(now time.Time) time.Duration {
	if w == nil {
		return 0
	}
	return now.Sub(time.Unix(0, w.last.Load()))
}

// watchStall calls cancel when no byte moved for timeout, until stop is called.
func watchStall(w *stallWatch, timeout time.Duration, cancel func()) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(timeout / 6)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				if w.stalledFor(now) >= timeout {
					cancel()
					return
				}
			}
		}
	}()
	return func() { close(done) }
}
