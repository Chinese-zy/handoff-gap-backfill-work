package gap

import (
	"context"
	"net"
	"sync"
	"time"
)

// ClientHandler receives data records for one connection. The client
// Ack method must be called with conn (typically after successful
// processing) to confirm a record; every reconnect resumes from the
// last confirmed sequence.
type ClientHandler func(conn net.Conn, e *Entry) error

// Client is a reconnecting consumer for the fixed-record protocol.
type Client struct {
	addr    string
	handler ClientHandler
	fail    FailPoint

	mu   sync.Mutex
	ackd uint64
}

// Dial connects to addr, announces lastConfirmed and streams DATA
// records to handler. Handlers confirm records through the returned
// Client.Ack.
func Dial(ctx context.Context, addr string, handler ClientHandler) (*Client, error) {
	c := &Client{addr: addr, handler: handler}
	if err := c.runOnce(ctx, 0); err != nil {
		return nil, err
	}
	return c, nil
}

// Resume reconnects and resumes from the last locally confirmed
// sequence, so the server first backfills exactly the missing records.
func (c *Client) Resume(ctx context.Context) error {
	backoff := 10 * time.Millisecond
	for {
		err := c.runOnce(ctx, c.lastAck())
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// InjectFailPoint installs a receive-side failure hook. The hook is
// evaluated before the handler runs; a returned error fails that
// delivery attempt and the record is not confirmed.
func (c *Client) InjectFailPoint(fp FailPoint) { c.fail = fp }

// Ack records seq as confirmed and forwards it to the server. A
// backwards sequence is never sent and never lowers the local
// watermark.
func (c *Client) Ack(conn net.Conn, seq uint64) error {
	if !c.advanceAck(seq) {
		return nil
	}
	return writeRecord(conn, tagAck, seq, nil)
}

func (c *Client) runOnce(ctx context.Context, lastConfirmed uint64) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeRecord(conn, tagHello, lastConfirmed, nil); err != nil {
		return err
	}

	var payload [PayloadSize]byte
	for {
		tag, seq, err := readRecord(conn, &payload)
		if err != nil {
			return err
		}
		if tag != tagData {
			continue
		}
		if c.fail != nil {
			if err := c.fail("receive", seq); err != nil {
				return err
			}
		}
		e := &Entry{Seq: seq, Payload: payload}
		if err := c.handler(conn, e); err != nil {
			return err
		}
	}
}

func (c *Client) lastAck() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ackd
}

func (c *Client) advanceAck(seq uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq <= c.ackd {
		return false
	}
	c.ackd = seq
	return true
}
