package syncer

import (
	"sync"
	"time"
)

// fileHashCache keeps a freshly computed digest for the next few scans. When a
// file changes, the scan has to read it once to learn its SHA-256; that same
// digest is usually the one the next scan would compute, because the scan
// before the transfer used metadata only. Prefetching it means the next scan
// can continue to be a metadata walk instead of reading every changed file a
// third time. Entries are keyed by virtual path and trusted only while the
// size and millisecond mtime still match.
type fileHashCache struct {
	mu      sync.Mutex
	entries map[string]*hashEntry
}

type hashEntry struct {
	size  int64
	mtime int64
	hash  string
	ready bool
	done  chan struct{}
}

type hashJob struct {
	rel      string
	fullPath string
	size     int64
	mtime    int64
	hash     string
}

// hashPrefetchSem bounds background hashing so a large first scan cannot
// compete with the transfers it is preparing.
var hashPrefetchSem = make(chan struct{}, 2)

func newFileHashCache() *fileHashCache {
	return &fileHashCache{entries: make(map[string]*hashEntry, 256)}
}

func hashMetaMatches(e *hashEntry, size, mtime int64) bool {
	return e != nil && e.size == size && abs64(e.mtime-mtime) <= 2
}

// take returns a prefetched digest when one matches this scan's metadata.
func (c *fileHashCache) take(rel string, size, mtime int64) (string, bool) {
	if c == nil {
		return "", false
	}
	c.ensure()
	c.mu.Lock()
	e := c.entries[rel]
	ok := hashMetaMatches(e, size, mtime) && e.ready && e.hash != ""
	hash := ""
	if ok {
		hash = e.hash
	}
	c.mu.Unlock()
	return hash, ok
}

// ensure creates the map for a cache built as a zero value.
func (c *fileHashCache) ensure() {
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*hashEntry, 64)
	}
	c.mu.Unlock()
}

// hashOrCompute waits for a prefetch of the same file or reads it itself.
func (c *fileHashCache) hashOrCompute(rel, fullPath string, size, mtime int64) (string, error) {
	if c != nil {
		c.ensure()
		c.mu.Lock()
		e := c.entries[rel]
		if hashMetaMatches(e, size, mtime) {
			if e.ready && e.hash != "" {
				hash := e.hash
				c.mu.Unlock()
				return hash, nil
			}
			done := e.done
			c.mu.Unlock()
			if done != nil {
				<-done
			}
			c.mu.Lock()
			e = c.entries[rel]
			ok := hashMetaMatches(e, size, mtime) && e.ready && e.hash != ""
			hash := ""
			if ok {
				hash = e.hash
			}
			c.mu.Unlock()
			if ok {
				return hash, nil
			}
		} else {
			c.mu.Unlock()
		}
	}
	return HashFile(fullPath)
}

// prefetch computes digests in the background for files whose bytes just
// changed. The scan itself never waits for these; it uses the digest it
// already has.
func (c *fileHashCache) prefetch(jobs []hashJob) {
	if c == nil || len(jobs) == 0 {
		return
	}
	work := make([]hashJob, 0, len(jobs))
	c.ensure()
	c.mu.Lock()
	for _, job := range jobs {
		if job.rel == "" || job.fullPath == "" {
			continue
		}
		if e, ok := c.entries[job.rel]; ok && hashMetaMatches(e, job.size, job.mtime) {
			continue
		}
		done := make(chan struct{})
		c.entries[job.rel] = &hashEntry{size: job.size, mtime: job.mtime, done: done}
		work = append(work, job)
	}
	c.mu.Unlock()
	if len(work) == 0 {
		return
	}

	go func() {
		queue := make(chan hashJob)
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				for job := range queue {
					hashPrefetchSem <- struct{}{}
					hash, err := HashFile(job.fullPath)
					<-hashPrefetchSem

					c.mu.Lock()
					e := c.entries[job.rel]
					if hashMetaMatches(e, job.size, job.mtime) {
						if err == nil && hash != "" {
							e.hash = hash
						}
						e.ready = true
						if e.done != nil {
							close(e.done)
						}
					}
					c.mu.Unlock()
				}
			}()
		}
		for _, job := range work {
			queue <- job
		}
		close(queue)
		wg.Wait()
	}()
}

// retain drops cached digests for paths not seen as changed by the current
// scan, keeping the cache bounded by the active change set.
func (c *fileHashCache) retain(jobs []hashJob) {
	if c == nil {
		return
	}
	keep := make(map[string]struct{}, len(jobs))
	for _, j := range jobs {
		keep[j.rel] = struct{}{}
	}
	c.ensure()
	c.mu.Lock()
	for rel, e := range c.entries {
		if _, ok := keep[rel]; ok {
			continue
		}
		if e.ready || e.done == nil {
			delete(c.entries, rel)
		}
	}
	c.mu.Unlock()
}

// waitForPrefetch gives an in-progress background hash a short window to
// finish before a transfer reads the file again. It exists so upload paths can
// benefit from work already underway without blocking indefinitely.
func (c *fileHashCache) waitForPrefetch(rel string, size, mtime int64, timeout time.Duration) (string, bool) {
	if c == nil {
		return "", false
	}
	c.ensure()
	c.mu.Lock()
	e := c.entries[rel]
	if !hashMetaMatches(e, size, mtime) || e.ready || e.done == nil {
		hash := ""
		ok := hashMetaMatches(e, size, mtime) && e.ready && e.hash != ""
		if ok {
			hash = e.hash
		}
		c.mu.Unlock()
		return hash, ok
	}
	done := e.done
	c.mu.Unlock()

	select {
	case <-done:
		return c.take(rel, size, mtime)
	case <-time.After(timeout):
		return "", false
	}
}
