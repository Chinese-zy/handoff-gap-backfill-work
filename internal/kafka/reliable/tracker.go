// Package reliable implements ordered, gap-aware message redelivery.
//
// A Tracker tracks messages by a monotonically increasing sequence number.
// Messages are handed to the consumer in strict sequence order: a message
// whose predecessor has not been confirmed yet is parked in the gap buffer
// instead of being delivered out of order. After a disconnect/reconnect the
// consumer resumes from the last confirmed sequence number and the missing
// stretch is redelivered first; messages arriving meanwhile cannot jump ahead
// of the gap.
package reliable

import (
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrAlreadyConfirmed is returned when a message with a sequence number
	// at or below the high-water mark is offered again.
	ErrAlreadyConfirmed = errors.New("message already confirmed")
	// ErrBacklogFull is returned when the in-flight window is full and the
	// caller must stop offering new messages. The offered message is not
	// accepted; callers must keep it and retry later, nothing is dropped.
	ErrBacklogFull = errors.New("unconfirmed backlog full")
)

// Message is the unit tracked by Tracker.
type Message struct {
	Seq   uint64
	Value []byte
}

// Tracker delivers messages strictly in sequence order and redelivers the
// unconfirmed stretch from the last confirmed sequence number.
//
// Semantics:
//   - Delivery resumes at confirmed+1; confirmed messages are never delivered
//     again (Offer rejects them with ErrAlreadyConfirmed).
//   - Messages arriving while a gap exists are buffered and never delivered
//     ahead of the gap; they become eligible in seq order once the gap closes.
//   - Confirmation numbers can only move forward; a Confirm at or below the
//     high-water mark is ignored and never re-emits confirmed messages.
//   - When the unconfirmed window reaches MaxPending, Offer rejects *new*
//     (never-seen) messages with ErrBacklogFull. Buffered unconfirmed
//     messages are retained. Re-offering an already pending message is a
//     no-op success so retries are idempotent.
type Tracker struct {
	// Confirmed is the high-water mark: every seq <= Confirmed is done.
	Confirmed  uint64
	MaxPending int

	// pending holds every unconfirmed message keyed by seq, including the
	// messages behind an open gap and messages ahead of it.
	pending map[uint64][]byte
}

// NewTracker creates a Tracker that resumes after confirmed and holds at most
// maxPending unconfirmed messages. A non-positive maxPending means unlimited.
func NewTracker(confirmed uint64, maxPending int) *Tracker {
	return &Tracker{
		Confirmed:  confirmed,
		MaxPending: maxPending,
		pending:    make(map[uint64][]byte),
	}
}

// Pending returns the number of unconfirmed messages currently retained.
func (t *Tracker) Pending() int { return len(t.pending) }

// Offer accepts a message for delivery.
//
// Messages at or below the high-water mark are rejected with
// ErrAlreadyConfirmed so they are never processed twice. A new message that
// would exceed MaxPending is rejected with ErrBacklogFull and must be retried
// later; it is not stored and nothing is evicted. Re-offering a seq already
// pending succeeds without changing state.
func (t *Tracker) Offer(msg Message) error {
	if msg.Seq <= t.Confirmed {
		return fmt.Errorf("seq %d <= confirmed %d: %w", msg.Seq, t.Confirmed, ErrAlreadyConfirmed)
	}
	if _, ok := t.pending[msg.Seq]; ok {
		return nil
	}
	if t.MaxPending > 0 && len(t.pending) >= t.MaxPending {
		return fmt.Errorf("backlog %d >= limit %d: %w", len(t.pending), t.MaxPending, ErrBacklogFull)
	}
	t.pending[msg.Seq] = clone(msg.Value)
	return nil
}

// Drain returns all currently deliverable messages in ascending seq order
// starting at confirmed+1, stopping at the first gap. Newly offered messages
// stay behind an open gap and therefore cannot overtake it.
func (t *Tracker) Drain() []Message {
	if len(t.pending) == 0 {
		return nil
	}
	seqs := make([]uint64, 0, len(t.pending))
	for seq := range t.pending {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	var out []Message
	next := t.Confirmed + 1
	for _, seq := range seqs {
		if seq != next {
			break // gap: everything after it waits
		}
		out = append(out, Message{Seq: seq, Value: clone(t.pending[seq])})
		next++
	}
	return out
}

// Confirm advances the high-water mark to seq when it is greater than the
// current mark and drops every confirmed message from the pending window. A
// backwards (or equal) confirmation is ignored: previously confirmed
// messages are not re-emitted. It returns the messages that became newly
// deliverable as a result, in ascending seq order, stopping at the next gap.
func (t *Tracker) Confirm(seq uint64) []Message {
	if seq <= t.Confirmed {
		return nil
	}
	for s := range t.pending {
		if s <= seq {
			delete(t.pending, s)
		}
	}
	t.Confirmed = seq
	return t.Drain()
}

// Gap reports the sequence number currently missing (confirmed+1) and whether
// it is present in the window. When false, reconnect/resume must redeliver
// starting at that seq before anything after it may be processed.
func (t *Tracker) Gap() (uint64, bool) {
	need := t.Confirmed + 1
	_, ok := t.pending[need]
	return need, ok
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
