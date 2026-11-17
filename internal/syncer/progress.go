package syncer

import "io"

// progressWriter feeds the live throughput meter while a body is copied.
// It wraps the destination, not the source, so the count is what actually
// reached the file (or the socket) rather than what was merely offered.
type progressWriter struct {
	w      io.Writer
	onMove func(int64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 && p.onMove != nil {
		p.onMove(int64(n))
	}
	return n, err
}

// progressReader counts bytes as the transport reads them from a local file,
// which makes an upload's rate live instead of known only at completion.
type progressReader struct {
	r      io.Reader
	onMove func(int64)
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 && p.onMove != nil {
		p.onMove(int64(n))
	}
	return n, err
}
