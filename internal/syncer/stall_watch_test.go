package syncer

import (
	"testing"
	"time"
)

// A transfer that stops moving must be noticed in minutes, not left waiting
// for the per-file deadline that the log showed as "stuck with no speed".
func TestStallWatchFiresAfterTimeout(t *testing.T) {
	fired := make(chan struct{}, 1)
	stall := newStallWatch()
	stop := watchStall(stall, 300*time.Millisecond, func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	defer stop()

	select {
	case <-fired:
		// The default checker ticks at timeout/6, so this is expected within
		// roughly 300ms; the test simply waits for it.
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog never fired for a stalled transfer")
	}
}

func TestStallWatchStaysQuietWhileBytesMove(t *testing.T) {
	fired := make(chan struct{}, 1)
	stall := newStallWatch()
	stop := watchStall(stall, 400*time.Millisecond, func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	defer stop()

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stall.touch()
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-fired:
		t.Fatal("watchdog fired while the transfer was still moving")
	default:
	}
}

func TestTransferTimeoutAllowsSlowLargeFiles(t *testing.T) {
	// 2 GB at the conservative 100 KiB/s estimate is well over an hour; the
	// old 60-minute ceiling cut such uploads off.
	if got := transferTimeout(2 << 30); got < 2*time.Hour {
		t.Fatalf("transferTimeout(2GiB) = %s; want at least 2h", got)
	}
	if transferTimeout(0) != 2*time.Minute {
		t.Fatalf("transferTimeout(0) = %s", transferTimeout(0))
	}
}
