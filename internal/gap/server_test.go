package gap

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func dialHello(t *testing.T, addr string, lastConfirmed uint64) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := writeRecord(conn, tagHello, lastConfirmed, nil); err != nil {
		t.Fatalf("hello: %v", err)
	}
	return conn
}

func readData(t *testing.T, conn net.Conn) (uint64, [PayloadSize]byte) {
	t.Helper()
	var payload [PayloadSize]byte
	tag, seq, err := readRecord(conn, &payload)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if tag != tagData {
		t.Fatalf("tag = %d, want DATA", tag)
	}
	return seq, payload
}

func publishN(t *testing.T, srv *Server, from, to uint64) {
	t.Helper()
	ctx := context.Background()
	for i := from; i <= to; i++ {
		if _, err := srv.Publish(ctx, fixedPayload(i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

func TestReconnectBackfillsInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := Listen(ctx, "127.0.0.1:0", 32)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	publishN(t, srv, 1, 5)

	// First connection: read 1..3, confirm 3, then drop the line
	// without touching the server process.
	conn := dialHello(t, srv.Addr(), 0)
	for i := uint64(1); i <= 3; i++ {
		seq, payload := readData(t, conn)
		if seq != i {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
		expect := fixedPayload(i)
		for b := 0; b < PayloadSize; b++ {
			if payload[b] != expect[b] {
				t.Fatalf("payload mismatch at seq %d", seq)
			}
		}
	}
	if err := writeRecord(conn, tagAck, 3, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the server process the ack
	_ = conn.Close()

	// New entries arriving during the outage cannot jump the queue.
	publishN(t, srv, 6, 8)

	// Reconnect from the last confirmed sequence number.
	conn2 := dialHello(t, srv.Addr(), 3)
	defer conn2.Close()

	var got []uint64
	for i := 0; i < 5; i++ {
		seq, _ := readData(t, conn2)
		got = append(got, seq)
	}
	want := []uint64{4, 5, 6, 7, 8}
	for i, s := range want {
		if got[i] != s {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// A backwards ack is accepted on the wire but must not replay.
	if err := writeRecord(conn2, tagAck, 2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := srv.Log().Confirmed(); got != 3 {
		t.Fatalf("watermark = %d, still 3 after backwards ack", got)
	}

	if err := writeRecord(conn2, tagAck, 8, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for srv.Log().Backlog() != 0 {
		select {
		case <-deadline:
			t.Fatalf("backlog = %d, want 0", srv.Log().Backlog())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// A brand new connection after full confirmation gets nothing old.
	conn3 := dialHello(t, srv.Addr(), 8)
	defer conn3.Close()
	_ = conn3.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var p [PayloadSize]byte
	if _, _, err := readRecord(conn3, &p); err != io.EOF && !isTimeout(err) && err != io.ErrUnexpectedEOF {
		if err == nil {
			t.Fatal("confirmed entries were delivered again")
		}
	}
}

func isTimeout(err error) bool {
	type timeout interface{ Timeout() bool }
	te, ok := err.(timeout)
	return ok && te.Timeout()
}

func TestInjectedSendFailureRetriesSameRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := Listen(ctx, "127.0.0.1:0", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	var mu sync.Mutex
	failed := false
	srv.InjectFailPoint(func(point string, seq uint64) error {
		mu.Lock()
		defer mu.Unlock()
		if point == "send" && seq == 2 && !failed {
			failed = true
			return errInjected
		}
		return nil
	})

	publishN(t, srv, 1, 3)

	conn := dialHello(t, srv.Addr(), 0)
	defer conn.Close()

	var got []uint64
	for i := 0; i < 3; i++ {
		seq, _ := readData(t, conn)
		got = append(got, seq)
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("got %v, want [1 2 3] despite injected failure on 2", got)
	}
}

var errInjected = &timeoutErr{"injected failure"}

type timeoutErr struct{ msg string }

func (e *timeoutErr) Error() string { return e.msg }
