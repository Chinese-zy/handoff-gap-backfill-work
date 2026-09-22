package reliable

// Source simulates a broker stream without touching any real process.
// It holds a growing log of fixed-size records. A session reads the log
// starting at an arbitrary offset; failure points are injected as dropped
// seqs so a reconnect demonstrably leaves a gap that must be backfilled.
type Source struct {
	log map[uint64][]byte
	max uint64
}

// NewSource creates an empty source log.
func NewSource() *Source { return &Source{log: make(map[uint64][]byte)} }

// Append produces one more fixed-size record and returns its seq.
func (s *Source) Append() uint64 {
	s.max++
	s.log[s.max] = Encode(s.max)
	return s.max
}

// Last returns the newest seq present in the log.
func (s *Source) Last() uint64 { return s.max }

// FailurePoint decides, for a given seq during a session, whether delivery is
// lost. It is the only injected failure seam: no process is killed, the
// session simply skips that record until Reconnect resumes from it.
type FailurePoint func(seq uint64) bool

// Session is one connected read over the source log.
type Session struct {
	src     *Source
	cursor  uint64
	failing FailurePoint
	closed  bool
}

// OpenSession starts reading at from (inclusive). The failure hook may be nil.
func (s *Source) OpenSession(from uint64, hook FailurePoint) *Session {
	return &Session{src: s, cursor: from, failing: hook}
}

// Reconnect closes the current session (if any) and opens a fresh one that
// resumes exactly at the last confirmed seq + 1.
func Reconnect(t *Tracker, s *Source, hook FailurePoint) *Session {
	return s.OpenSession(t.Confirmed+1, hook)
}

// Pull returns up to one currently available record that is not suppressed by
// the failure point, advancing the cursor. The second result is false at the
// end of the log: the caller should Append more, Confirm progress, or
// Reconnect. Reconnecting is the only way to reach a record that the failure
// point skipped, mirroring gap backfill.
func (sess *Session) Pull() (Message, bool) {
	if sess.closed {
		return Message{}, false
	}
	for seq := sess.cursor; seq <= sess.src.max; seq++ {
		sess.cursor = seq + 1
		if sess.failing != nil && sess.failing(seq) {
			continue // injected loss: record is skipped this session
		}
		return Message{Seq: seq, Value: sess.src.log[seq]}, true
	}
	return Message{}, false
}

// Close ends the session.
func (sess *Session) Close() { sess.closed = true }
