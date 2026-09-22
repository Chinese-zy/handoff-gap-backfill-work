package gap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func fixedPayload(seq uint64) []byte {
	p := make([]byte, PayloadSize)
	for i := range p {
		p[i] = byte(seq) ^ byte(i)
	}
	return p
}

func collectAsync(ctx context.Context, t *testing.T, l *Log, from uint64) func() []uint64 {
	t.Helper()
	mu := sync.Mutex{}
	got := make([]uint64, 0)
	go func() {
		err := l.Subscribe(ctx, from, SinkFunc(func(_ context.Context, e *Entry) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, e.Seq)
			return nil
		}))
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("subscribe: %v", err)
		}
	}()
	return func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint64(nil), got...)
	}
}

func waitSeqs(t *testing.T, snapshot func() []uint64, cancel context.CancelFunc, want []uint64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		default:
			got := snapshot()
			if len(got) >= len(want) {
				cancel()
				for i, s := range want {
					if got[i] != s {
						t.Fatalf("got %v, want %v", got, want)
					}
				}
				return
			}
		case <-deadline:
			cancel()
			t.Fatalf("timeout waiting for %v, got %v", want, snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPublishFixedSize(t *testing.T) {
	l := NewLog(4)
	ctx := context.Background()

	if _, err := l.Publish(ctx, make([]byte, PayloadSize-1)); !errors.Is(err, ErrPayloadSize) {
		t.Fatalf("want ErrPayloadSize, got %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		e, err := l.Publish(ctx, fixedPayload(i))
		if err != nil {
			t.Fatal(err)
		}
		if e.Seq != i {
			t.Fatalf("seq = %d, want %d", e.Seq, i)
		}
	}
}

func TestSubscribeBackfillsGapThenNew(t *testing.T) {
	l := NewLog(20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := uint64(1); i <= 5; i++ {
		if _, err := l.Publish(ctx, fixedPayload(i)); err != nil {
			t.Fatal(err)
		}
	}

	mu := sync.Mutex{}
	got := make([]uint64, 0)
	go func() {
		_ = l.Subscribe(ctx, 2, SinkFunc(func(_ context.Context, e *Entry) error {
			mu.Lock()
			got = append(got, e.Seq)
			mu.Unlock()
			return nil
		}))
	}()

	snapshot := func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint64(nil), got...)
	}
	deadline := time.After(2 * time.Second)
	for len(snapshot()) < 3 {
		select {
		case <-deadline:
			t.Fatalf("got %v, want backfill of 3,4,5", snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	backfill := snapshot()
	want := []uint64{3, 4, 5}
	for i, s := range want {
		if backfill[i] != s {
			t.Fatalf("got %v, want prefix %v", backfill, want)
		}
	}

	// New arrivals must follow the backfill in strict order.
	for i := uint64(6); i <= 8; i++ {
		if _, err := l.Publish(ctx, fixedPayload(i)); err != nil {
			t.Fatal(err)
		}
	}
	for len(snapshot()) < 6 {
		time.Sleep(time.Millisecond)
	}
	all := snapshot()
	for i, s := range []uint64{3, 4, 5, 6, 7, 8} {
		if all[i] != s {
			t.Fatalf("got %v, want strictly ascending delivery", all)
		}
	}
}

func TestAckRemovesAndNeverReplays(t *testing.T) {
	l := NewLog(10)
	ctx := context.Background()
	for i := uint64(1); i <= 4; i++ {
		_, _ = l.Publish(ctx, fixedPayload(i))
	}

	if got := l.Ack(2); got != 2 {
		t.Fatalf("watermark = %d, want 2", got)
	}
	if l.Backlog() != 2 {
		t.Fatalf("backlog = %d, want 2", l.Backlog())
	}

	subCtx, cancel := context.WithCancel(ctx)
	waitSeqs(t, collectAsync(subCtx, t, l, 2), cancel, []uint64{3, 4})
}

func TestBackwardsAckIsIgnored(t *testing.T) {
	l := NewLog(10)
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		_, _ = l.Publish(ctx, fixedPayload(i))
	}
	l.Ack(3)
	if got := l.Ack(1); got != 3 {
		t.Fatalf("backwards ack moved watermark: %d", got)
	}
	if l.Backlog() != 0 {
		t.Fatalf("backlog = %d, want 0", l.Backlog())
	}

	subCtx, cancel := context.WithCancel(ctx)
	out := collectAsync(subCtx, t, l, 0)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	if got := out(); len(got) != 0 {
		t.Fatalf("resumed ack resurrected confirmed entries: %v", got)
	}
}

func TestCapStopsNewWithoutDroppingUnconfirmed(t *testing.T) {
	l := NewLog(2)
	ctx := context.Background()

	_, _ = l.Publish(ctx, fixedPayload(1))
	_, _ = l.Publish(ctx, fixedPayload(2))

	published := make(chan uint64, 1)
	go func() {
		e, err := l.Publish(ctx, fixedPayload(3))
		if err == nil {
			published <- e.Seq
		}
	}()

	select {
	case seq := <-published:
		t.Fatalf("publish over cap succeeded with seq %d", seq)
	case <-time.After(50 * time.Millisecond):
	}
	if l.Backlog() != 2 {
		t.Fatalf("backlog = %d, want 2 (unconfirmed retained)", l.Backlog())
	}

	// Confirm one entry: the blocked publish now proceeds and the
	// previously retained entries are untouched.
	l.Ack(1)
	select {
	case seq := <-published:
		if seq != 3 {
			t.Fatalf("seq = %d, want 3", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("publish stayed blocked after ack freed a slot")
	}
	if l.Backlog() != 2 {
		t.Fatalf("backlog = %d, want 2", l.Backlog())
	}

	subCtx, cancel := context.WithCancel(ctx)
	waitSeqs(t, collectAsync(subCtx, t, l, 1), cancel, []uint64{2, 3})
}

func TestSinkFailureRetriesSameSeq(t *testing.T) {
	l := NewLog(4)
	ctx := context.Background()
	_, _ = l.Publish(ctx, fixedPayload(1))
	_, _ = l.Publish(ctx, fixedPayload(2))

	var mu sync.Mutex
	var attempts map[uint64]int
	attempts = map[uint64]int{}
	var got []uint64

	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		_ = l.Subscribe(subCtx, 0, SinkFunc(func(_ context.Context, e *Entry) error {
			mu.Lock()
			defer mu.Unlock()
			attempts[e.Seq]++
			if e.Seq == 1 && attempts[e.Seq] == 1 {
				return errors.New("injected send failure")
			}
			got = append(got, e.Seq)
			return nil
		}))
	}()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := len(got) >= 2
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("got %v attempts %v", got, attempts)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	mu.Lock()
	defer mu.Unlock()
	if attempts[1] != 2 {
		t.Fatalf("seq 1 attempts = %d, want 2", attempts[1])
	}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
}
