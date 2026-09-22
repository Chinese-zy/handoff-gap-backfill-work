package gap

import (
	"context"
	"net"
	"sync"
)

// FailPoint is an injectable failure used by tests. It is invoked at a
// named point with the current sequence number; returning a non-nil
// error makes that point fail once. No process is killed and no real
// connection is torn down by the library itself.
type FailPoint func(point string, seq uint64) error

// Server fronts a Log over TCP using the fixed record protocol.
type Server struct {
	log    *Log
	fail   FailPoint
	ln     net.Listener
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// Listen starts a server on addr (e.g. ":9101"). The same address is
// always used; reconnecting clients return to the same intake port.
func Listen(ctx context.Context, addr string, maxBacklog int) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{log: NewLog(maxBacklog), ln: ln}
	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return s, nil
}

// Addr returns the address the server is actually listening on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Log exposes the backing log for publishing.
func (s *Server) Log() *Log { return s.log }

// InjectFailPoint installs a failure hook applied to every connection.
// It must be called before clients connect.
func (s *Server) InjectFailPoint(fp FailPoint) { s.fail = fp }

// Close stops accepting and releases resources.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	err := s.ln.Close()
	s.log.Close()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.serve(ctx, conn)
	}
}

func (s *Server) serve(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	var payload [PayloadSize]byte
	tag, lastConfirmed, err := readRecord(conn, &payload)
	if err != nil || tag != tagHello {
		return
	}

	ackCh := make(chan uint64, 64)
	ackDone := make(chan struct{})
	go s.readAcks(conn, ackCh, ackDone)

	// Drain incoming acks while sending. Backwards acks are ignored
	// inside Log.Ack; already-confirmed entries are never replayed.
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sink := SinkFunc(func(_ context.Context, e *Entry) error {
		if s.fail != nil {
			if err := s.fail("send", e.Seq); err != nil {
				return err
			}
		}
		return writeRecord(conn, tagData, e.Seq, e.Payload[:])
	})

	done := make(chan struct{})
	go func() {
		_ = s.log.Subscribe(sendCtx, lastConfirmed, sink)
		close(done)
	}()

	for {
		select {
		case seq := <-ackCh:
			s.log.Ack(seq)
		case <-ackDone:
			cancel()
			<-done
			return
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) readAcks(conn net.Conn, out chan<- uint64, done chan<- struct{}) {
	defer close(done)
	var payload [PayloadSize]byte
	for {
		tag, seq, err := readRecord(conn, &payload)
		if err != nil {
			return
		}
		if tag != tagAck {
			continue
		}
		out <- seq
	}
}

// Publish is a convenience wrapper around the backing log.
func (s *Server) Publish(ctx context.Context, payload []byte) (*Entry, error) {
	return s.log.Publish(ctx, payload)
}
