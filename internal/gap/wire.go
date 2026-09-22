package gap

import (
	"encoding/binary"
	"errors"
	"io"
)

// Fixed-size on-wire records. Every record starts with a one-byte tag
// followed by an 8-byte big-endian sequence number. DATA records append
// exactly PayloadSize bytes, so a DATA record is 41 bytes.
const (
	tagHello uint8 = 1
	tagData  uint8 = 2
	tagAck   uint8 = 3
)

const (
	headerSize = 1 + 8
	dataSize   = headerSize + PayloadSize
)

var (
	errShortRecord = errors.New("gap: short record")
	errBadTag      = errors.New("gap: unknown record tag")
)

func writeRecord(w io.Writer, tag uint8, seq uint64, payload []byte) error {
	buf := make([]byte, 0, dataSize)
	buf = append(buf, tag)
	buf = binary.BigEndian.AppendUint64(buf, seq)
	if tag == tagData {
		if len(payload) != PayloadSize {
			return ErrPayloadSize
		}
		buf = append(buf, payload...)
	}
	_, err := w.Write(buf)
	return err
}

// readRecord parses one full record from r. For DATA records payload is
// copied into the 32-byte array returned by the caller.
func readRecord(r io.Reader, payload *[PayloadSize]byte) (tag uint8, seq uint64, err error) {
	var header [headerSize]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return 0, 0, err
	}
	tag = header[0]
	seq = binary.BigEndian.Uint64(header[1:])
	switch tag {
	case tagHello, tagAck:
		return tag, seq, nil
	case tagData:
		if _, err = io.ReadFull(r, payload[:]); err != nil {
			return 0, 0, err
		}
		return tag, seq, nil
	default:
		return 0, 0, errBadTag
	}
}
