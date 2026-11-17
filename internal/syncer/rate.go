package syncer

import (
	"sync"
	"time"
)

const (
	// rateWindow is how far back a throughput sample counts. Two seconds is
	// long enough that a single small file cannot read as a gigabyte per
	// second, and short enough that the number follows a transfer as it runs.
	rateWindow    = 2 * time.Second
	rateSlotWidth = 100 * time.Millisecond
	rateSlots     = int(rateWindow / rateSlotWidth)
)

type rateSlot struct {
	start int64 // Unix milliseconds at the start of the bucket
	bytes int64
}

// rateMeter computes a sliding-window transfer rate from the bytes actually
// read or written. A completed-file average (bytes / duration of one request)
// overstates real throughput for small files and freezes between transfers;
// counting bytes as they move gives an honest, live number instead.
type rateMeter struct {
	mu      sync.Mutex
	buckets [rateSlots]rateSlot
	first   time.Time
}

func (m *rateMeter) reset() {
	m.mu.Lock()
	m.buckets = [rateSlots]rateSlot{}
	m.first = time.Time{}
	m.mu.Unlock()
}

// add records n bytes that moved at now.
func (m *rateMeter) add(n int64, now time.Time) {
	if n <= 0 || now.IsZero() {
		return
	}
	slot := int((now.UnixMilli() / rateSlotWidth.Milliseconds()) % int64(rateSlots))
	start := now.UnixMilli() - now.UnixMilli()%rateSlotWidth.Milliseconds()

	m.mu.Lock()
	if m.first.IsZero() {
		m.first = now
	}
	b := &m.buckets[slot]
	if b.start != start {
		*b = rateSlot{start: start}
	}
	b.bytes += n
	m.mu.Unlock()
}

// rate returns bytes per second over the samples still inside the window.
// The divisor is the observed span, not the full window, so the first samples
// of a transfer are not reported at half their real speed.
func (m *rateMeter) rate(now time.Time) int64 {
	if now.IsZero() {
		return 0
	}
	cutoff := now.UnixMilli() - rateWindow.Milliseconds()

	m.mu.Lock()
	var total int64
	oldest := int64(0)
	for i := range m.buckets {
		b := &m.buckets[i]
		if b.bytes == 0 || b.start < cutoff {
			continue
		}
		total += b.bytes
		if oldest == 0 || b.start < oldest {
			oldest = b.start
		}
	}
	first := m.first
	m.mu.Unlock()

	if total <= 0 {
		return 0
	}
	span := now.Sub(first)
	if first.IsZero() || span > rateWindow {
		// The meter was idle long enough that "since first byte" would divide
		// fresh bytes by an old span; measure from the oldest live bucket.
		span = now.Sub(time.UnixMilli(oldest))
	}
	if span < rateSlotWidth {
		span = rateSlotWidth
	}
	if span > rateWindow {
		span = rateWindow
	}
	bps := int64(float64(total) / span.Seconds())
	if bps <= 0 {
		return 0
	}
	return bps
}
