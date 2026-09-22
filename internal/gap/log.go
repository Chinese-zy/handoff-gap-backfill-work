// Package gap implements an ordered byte stream with reconnect gap
// backfill.
//
// Delivery semantics:
//
//   - After a reconnect the receiver resumes from its last confirmed
//     sequence number: every retained entry with a higher sequence is
//     resent, in order, before anything published afterwards.
//   - New entries can never overtake the backfill: subscribers always
//     drain the log in strictly ascending sequence order.
//   - Confirmed entries are discarded once and never delivered again.
//   - Acknowledgement numbers are monotonic; an acknowledgement that
//     moves backwards is ignored and can never resurrect confirmed data.
//   - When the unconfirmed backlog reaches its cap, publishers block.
//     Unconfirmed entries are never dropped to make room.
package gap

import (
	"context"
	"errors"
	"sync"
)

// PayloadSize is the fixed frame payload size used by the wire protocol
// and by tests. Payloads smaller than PayloadSize are rejected.
const PayloadSize = 32

// ErrPayloadSize is returned when a published payload does not have the
// fixed PayloadSize length.
var ErrPayloadSize = errors.New("gap: payload must be exactly PayloadSize bytes")

// Entry is one fixed-size record in the backlog, identified by a
// strictly increasing, gap-free sequence number.
type Entry struct {
	Seq     uint64
	Payload [PayloadSize]byte
}

// Sink consumes entries. Implementations are free to fail transiently;
// a failed entry is retried with the same sequence number and never
// skipped.
type Sink interface {
	Send(context.Context, *Entry) error
}

// SinkFunc adapts a function to the Sink interface.
type SinkFunc func(context.Context, *Entry) error

// Send implements Sink.
func (f SinkFunc) Send(ctx context.Context, e *Entry) error { return f(ctx, e) }

// Log retains entries that have not been confirmed yet and fans them
// out to subscribers in sequence order.
type Log struct {
	mu      sync.Mutex
	cond    *sync.Cond
	entries []*Entry
	nextSeq uint64
	maxGap  int
	closed  bool
	confSeq uint64
}

// NewLog creates a Log whose unconfirmed backlog may hold at most
// maxBacklog entries. maxBacklog must be positive.
func NewLog(maxBacklog int) *Log {
	if maxBacklog <= 0 {
		maxBacklog = 1
	}
	l := &Log{
		nextSeq: 1,
		maxGap:  maxBacklog,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Publish appends payload to the backlog with the next sequence number.
// It blocks while the backlog is full; unconfirmed entries are never
// evicted, so publishing applies backpressure instead of dropping data.
func (l *Log) Publish(ctx context.Context, payload []byte) (*Entry, error) {
	if len(payload) != PayloadSize {
		return nil, ErrPayloadSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	for len(l.entries) >= l.maxGap {
		if l.closed {
			return nil, ErrClosed
		}
		if err := l.wait(ctx); err != nil {
			return nil, err
		}
	}
	if l.closed {
		return nil, ErrClosed
	}

	e := &Entry{Seq: l.nextSeq}
	copy(e.Payload[:], payload)
	l.entries = append(l.entries, e)
	l.nextSeq++
	l.cond.Broadcast()
	return e, nil
}

// Subscribe sends every retained entry with a sequence number greater
// than lastConfirmed, in ascending order, followed by every entry
// published afterwards. Reconnecting with the last confirmed sequence
// therefore backfills exactly the missing prefix first.
//
// Send errors are retried against the same entry; the cursor only
// advances after the sink accepts the entry. Subscribe returns when ctx
// is cancelled or the log is closed.
func (l *Log) Subscribe(ctx context.Context, lastConfirmed uint64, sink Sink) error {
	go l.wakeOnCancel(ctx)

	cursor := lastConfirmed
	l.mu.Lock()
	defer l.mu.Unlock()

	for {
		if l.closed && len(l.entries) == 0 {
			return ErrClosed
		}
		e := l.firstAfter(cursor)
		if e == nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if l.closed {
				return ErrClosed
			}
			l.cond.Wait()
			continue
		}

		l.mu.Unlock()
		err := sink.Send(ctx, e)
		l.mu.Lock()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue // retry the same entry; nothing is skipped
		}
		cursor = e.Seq
	}
}

// Ack confirms all entries up to and including seq. The confirmation
// watermark only moves forwards: a seq lower than the current watermark
// is ignored, so acknowledged entries can never be resurrected and sent
// again. Confirmed prefix entries are removed from the backlog, which
// unblocks publishers waiting on the cap.
func (l *Log) Ack(seq uint64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if seq <= l.confSeq {
		return l.confSeq // backwards acknowledgement: no-op
	}
	l.confSeq = seq

	cut := 0
	for cut < len(l.entries) && l.entries[cut].Seq <= seq {
		cut++
	}
	if cut > 0 {
		l.entries = append(l.entries[:0], l.entries[cut:]...)
		l.cond.Broadcast()
	}
	return l.confSeq
}

// Confirmed returns the current confirmation watermark.
func (l *Log) Confirmed() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.confSeq
}

// Backlog returns the number of unconfirmed entries currently retained.
func (l *Log) Backlog() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Close stops publishers and subscribers. Retained entries stay
// observable until the log is closed, but no new ones are accepted.
func (l *Log) Close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *Log) firstAfter(seq uint64) *Entry {
	// entries are ascending and gap-free, so the first entry with
	// Seq > seq is either the head or reached after a short bounded
	// walk (the backlog is capped).
	for _, e := range l.entries {
		if e.Seq > seq {
			return e
		}
	}
	return nil
}

func (l *Log) wait(ctx context.Context) error {
	// cond.Wait has no context support; cancellation is surfaced by
	// wakeOnCancel broadcasting the condition.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			l.cond.Broadcast()
		case <-done:
		}
	}()
	l.cond.Wait()
	close(done)
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (l *Log) wakeOnCancel(ctx context.Context) {
	if ctx.Done() == nil {
		return
	}
	<-ctx.Done()
	l.cond.Broadcast()
}

// ErrClosed is returned by Log operations after Close.
var ErrClosed = errors.New("gap: log closed")
