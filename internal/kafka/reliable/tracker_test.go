package reliable

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadFixedBytesRoundTrip(t *testing.T) {
	b := Encode(42)
	require.Len(t, b, PayloadSize)

	seq, err := Decode(b)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), seq)

	corrupt := append([]byte(nil), b...)
	corrupt[10] = 'x' // same length, corrupt fill byte
	_, err = Decode(corrupt)
	assert.ErrorIs(t, err, ErrPayloadSize)
	_, err = Decode(b[:PayloadSize-1])
	assert.ErrorIs(t, err, ErrPayloadSize)
}

// Reconnect resumes from the last confirmed seq and the missing stretch is
// backfilled before newer messages are processed.
func TestResumeFromLastConfirmed_BackfillsGap(t *testing.T) {
	tr := NewTracker(0, 0)

	// First session: 1-3 arrive, only 1 is confirmed before disconnect.
	for _, seq := range []uint64{1, 2, 3} {
		require.NoError(t, tr.Offer(Message{Seq: seq, Value: Encode(seq)}))
	}
	got := tr.Drain()
	require.Equal(t, []uint64{1, 2, 3}, seqsOf(got))

	newly := tr.Confirm(1)
	assert.Equal(t, []uint64{2, 3}, seqsOf(newly))

	// Meanwhile a newer message (5) shows up while 4 is still missing.
	require.NoError(t, tr.Offer(Message{Seq: 5, Value: Encode(5)}))

	// Backfill 4 from the resumed stream; it must come before 5.
	require.NoError(t, tr.Offer(Message{Seq: 4, Value: Encode(4)}))
	got = tr.Drain()
	assert.Equal(t, []uint64{2, 3, 4, 5}, seqsOf(got))
}

// New messages cannot jump ahead of an open gap.
func TestNewMessagesCannotOvertakeGap(t *testing.T) {
	tr := NewTracker(3, 0) // 1..3 already confirmed
	require.NoError(t, tr.Offer(Message{Seq: 6, Value: Encode(6)}))
	require.NoError(t, tr.Offer(Message{Seq: 5, Value: Encode(5)}))

	assert.Empty(t, tr.Drain(), "nothing deliverable while seq 4 missing")
	need, ok := tr.Gap()
	assert.Equal(t, uint64(4), need)
	assert.False(t, ok)

	require.NoError(t, tr.Offer(Message{Seq: 4, Value: Encode(4)}))
	assert.Equal(t, []uint64{4, 5, 6}, seqsOf(tr.Drain()))
}

// Confirmed messages are never delivered twice.
func TestConfirmedMessageNeverRedelivered(t *testing.T) {
	tr := NewTracker(2, 0)

	err := tr.Offer(Message{Seq: 2, Value: Encode(2)})
	assert.ErrorIs(t, err, ErrAlreadyConfirmed)
	err = tr.Offer(Message{Seq: 1, Value: Encode(1)})
	assert.ErrorIs(t, err, ErrAlreadyConfirmed)

	require.NoError(t, tr.Offer(Message{Seq: 3, Value: Encode(3)}))
	tr.Confirm(3)
	err = tr.Offer(Message{Seq: 3, Value: Encode(3)})
	assert.ErrorIs(t, err, ErrAlreadyConfirmed)
	assert.Empty(t, tr.Drain())
}

// A confirmation number that moves backwards (or repeats) is ignored and never
// re-emits already confirmed messages.
func TestConfirmRollbackIsIgnored(t *testing.T) {
	tr := NewTracker(0, 0)
	for seq := uint64(1); seq <= 5; seq++ {
		require.NoError(t, tr.Offer(Message{Seq: seq, Value: Encode(seq)}))
	}
	tr.Confirm(5)
	assert.Equal(t, uint64(5), tr.Confirmed)

	assert.Nil(t, tr.Confirm(5), "repeated confirm emits nothing")
	assert.Nil(t, tr.Confirm(2), "backward confirm emits nothing")
	assert.Nil(t, tr.Confirm(0))
	assert.Equal(t, uint64(5), tr.Confirmed, "high-water mark never retreats")
	assert.Equal(t, 0, tr.Pending())
}

// When the unconfirmed window hits the limit, only new intake is stopped:
// pending messages are retained and backfill still closes the gap.
func TestBacklogLimitStopsIntakeButKeepsUnconfirmed(t *testing.T) {
	const limit = 3
	tr := NewTracker(0, limit)
	require.NoError(t, tr.Offer(Message{Seq: 1, Value: Encode(1)}))
	require.NoError(t, tr.Offer(Message{Seq: 3, Value: Encode(3)}))
	require.NoError(t, tr.Offer(Message{Seq: 4, Value: Encode(4)}))

	err := tr.Offer(Message{Seq: 5, Value: Encode(5)})
	require.ErrorIs(t, err, ErrBacklogFull)
	assert.Equal(t, limit, tr.Pending(), "nothing dropped to make room")

	// Retrying an already pending message is idempotent and never rejected.
	require.NoError(t, tr.Offer(Message{Seq: 3, Value: Encode(3)}))

	// Even the backfill record counts as new intake while full: it is refused
	// and retried later; the window still holds 2,3,4 and nothing is dropped.
	err = tr.Offer(Message{Seq: 2, Value: Encode(2)})
	assert.ErrorIs(t, err, ErrBacklogFull)
	assert.ElementsMatch(t, []uint64{1, 3, 4}, seqsInPending(tr))

	// Only seq 1 is processable until the gap closes. Confirming it frees a
	// slot, after which backfill (2) and new intake (5) flow in seq order.
	assert.Equal(t, []uint64{1}, seqsOf(tr.Drain()))
	tr.Confirm(1)
	require.NoError(t, tr.Offer(Message{Seq: 2, Value: Encode(2)}))

	// Window is full again (2,3,4); new intake stays blocked until progress.
	err = tr.Offer(Message{Seq: 5, Value: Encode(5)})
	assert.ErrorIs(t, err, ErrBacklogFull)
	assert.Equal(t, []uint64{2, 3, 4}, seqsOf(tr.Drain()))

	tr.Confirm(3)
	require.NoError(t, tr.Offer(Message{Seq: 5, Value: Encode(5)}))
	assert.Equal(t, []uint64{4, 5}, seqsOf(tr.Drain()))
	tr.Confirm(5)
	assert.Equal(t, 0, tr.Pending(), "no leak of unconfirmed messages")
}

// End-to-end behaviour against the in-memory Source with injected failure
// points and a reconnect: no real process is killed.
func TestSessionReconnectWithInjectedGap(t *testing.T) {
	src := NewSource()
	for i := 0; i < 6; i++ {
		src.Append()
	}

	tr := NewTracker(0, 0)

	// First session drops seq 3 (injected failure point).
	sess := src.OpenSession(1, func(seq uint64) bool { return seq == 3 })
	for {
		msg, ok := sess.Pull()
		if !ok {
			break
		}
		require.NoError(t, tr.Offer(msg))
	}
	sess.Close()

	// 1,2 deliver; 3 missing so 4,5,6 wait even though they arrived.
	assert.Equal(t, []uint64{1, 2}, seqsOf(tr.Drain()))
	tr.Confirm(2)

	// Reconnect resumes exactly at confirmed+1 = 3 and gap is backfilled.
	sess = Reconnect(tr, src, nil)
	for {
		msg, ok := sess.Pull()
		if !ok {
			break
		}
		require.NoError(t, tr.Offer(msg))
	}
	sess.Close()

	assert.Equal(t, []uint64{3, 4, 5, 6}, seqsOf(tr.Drain()))

	// Payloads are intact fixed-byte records.
	for _, m := range tr.Drain() {
		seq, err := Decode(m.Value)
		require.NoError(t, err)
		assert.Equal(t, m.Seq, seq)
	}
}

func TestUnlimitedBacklogAcceptsEverything(t *testing.T) {
	tr := NewTracker(0, 0)
	for seq := uint64(1); seq <= 1000; seq++ {
		require.NoError(t, tr.Offer(Message{Seq: seq, Value: Encode(seq)}))
	}
	assert.Equal(t, 1000, tr.Pending())
}

func TestDrainReturnsCopies(t *testing.T) {
	tr := NewTracker(0, 0)
	require.NoError(t, tr.Offer(Message{Seq: 1, Value: Encode(1)}))
	got := tr.Drain()
	got[0].Value[0] = 0xFF // mutate delivered copy

	again := tr.Drain()
	seq, err := Decode(again[0].Value)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)
}

func seqsOf(msgs []Message) []uint64 {
	out := make([]uint64, len(msgs))
	for i, m := range msgs {
		out[i] = m.Seq
	}
	return out
}

func seqsInPending(tr *Tracker) []uint64 {
	seqs := make([]uint64, 0, len(tr.pending))
	for s := range tr.pending {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}
