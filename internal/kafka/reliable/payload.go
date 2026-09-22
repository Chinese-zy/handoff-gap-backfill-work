package reliable

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PayloadSize is the fixed on-wire size of every test record in bytes:
// 8-byte big-endian sequence number followed by a fixed fill pattern.
const PayloadSize = 32

const fillByte = byte('z')

// ErrPayloadSize is returned when a payload is not exactly PayloadSize bytes.
var ErrPayloadSize = errors.New("payload has unexpected size")

// Encode builds a fixed-size payload for seq. Tests use these deterministic
// bytes so deliveries, duplicates and gaps can be compared byte-for-byte.
func Encode(seq uint64) []byte {
	b := make([]byte, PayloadSize)
	binary.BigEndian.PutUint64(b[:8], seq)
	for i := 8; i < PayloadSize; i++ {
		b[i] = fillByte
	}
	return b
}

// Decode parses a fixed-size payload produced by Encode.
func Decode(b []byte) (uint64, error) {
	if len(b) != PayloadSize {
		return 0, fmt.Errorf("got %d bytes, want %d: %w", len(b), PayloadSize, ErrPayloadSize)
	}
	for i := 8; i < PayloadSize; i++ {
		if b[i] != fillByte {
			return 0, fmt.Errorf("bad fill byte at %d: %w", i, ErrPayloadSize)
		}
	}
	return binary.BigEndian.Uint64(b[:8]), nil
}
