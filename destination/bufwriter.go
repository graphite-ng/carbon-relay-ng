// like bufio.Writer (Copyright 2009 The Go Authors. All rights reserved.)
// but with instrumentation around flushing
// because this codebase is the only user:
// * doesn't implement the entire bufio.Writer api because it doesn't need to
// * and some simplifications can be made, less edgecases etc
package destination

import (
	"bytes"
	"github.com/Dieterbe/go-metrics"
	"github.com/graphite-ng/carbon-relay-ng/stats"
	"github.com/op/go-logging"
	"io"
	"time"
)

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	key                   string
	err                   error
	buf                   []byte
	n                     int
	wr                    io.Writer
	durationOverflowFlush metrics.Timer
}

// NewWriterSize returns a new Writer whose buffer has at least the specified
// size. If the argument io.Writer is already a Writer with large enough
// size, it returns the underlying Writer.
func NewWriter(w io.Writer, size int, key string) *Writer {
	if size <= 0 {
		panic("invalid size requested")
	}
	return &Writer{
		key: key,
		buf: make([]byte, size),
		wr:  w,
		durationOverflowFlush: stats.Timer("dest=" + key + ".what=durationFlush.type=overflow"),
	}
}

// Flush writes any buffered data to the underlying io.Writer.
func (b *Writer) Flush() error {
	err := b.flush()
	return err
}

func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	if log.IsEnabledFor(logging.INFO) {
		bufs := bytes.Split(b.buf[0:b.n], []byte{'\n'})
		for _, buf := range bufs {
			log.Debug("bufWriter %s flush-writing to tcp %s\n", b.key, buf)
		}
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer.
func (b *Writer) Available() int { return len(b.buf) - b.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *Writer) Buffered() int { return b.n }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *Writer) Write(p []byte) (nn int, err error) {
	for len(p) > b.Available() && b.err == nil {
		var n int
		if b.Buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			// we should measure this duration because it's equivalent to a flush
			start := time.Now()
			log.Debug("bufWriter %s writing to tcp %s\n", b.key, p)
			n, b.err = b.wr.Write(p)
			b.durationOverflowFlush.UpdateSince(start)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.durationOverflowFlush.Time(func() {
				b.flush()
			})
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}
