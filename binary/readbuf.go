package binary

import (
	"bufio"
	"encoding/binary"
	"io"
)

// errors of parsing.
var (
	ErrNotEnoughBytes      = NewProtocolError(nil, "not enough bytes available")
	ErrNotEnoughBytesWrite = NewProtocolError(nil, "not enough bytes available in buffer to write")
)

/*
 *  Protocol read functionality
 */

// Parser struct.
type Parser struct {
	binaryEndianness binary.ByteOrder
}

// NewParser constructor.
func NewParser(b binary.ByteOrder) Parser {
	return Parser{
		binaryEndianness: b,
	}
}

func (p *Parser) ParseUint8(b []byte) uint8 {
	_ = b[0] // bounds check hint to compiler;

	return b[0]
}

func (p *Parser) ParseUint16(b []byte) uint16 {
	return p.binaryEndianness.Uint16(b)
}

func (p *Parser) ParseUint32(b []byte) uint32 {
	return p.binaryEndianness.Uint32(b)
}

func (p *Parser) ParseUint64(b []byte) uint64 {
	return p.binaryEndianness.Uint64(b)
}

func (p *Parser) ParseInt8(b []byte) int8 {
	return int8(p.ParseUint8(b))
}

func (p *Parser) ParseInt16(b []byte) int16 {
	return int16(p.ParseUint16(b))
}

func (p *Parser) ParseInt32(b []byte) int32 {
	return int32(p.ParseUint32(b))
}

func (p *Parser) ParseInt64(b []byte) int64 {
	return int64(p.ParseUint64(b))
}

/*
 * Read buffer.
 */

// ReadBuffer struct.
type ReadBuffer struct {
	reader bufio.Reader
	Parser Parser
}

// NewReadBuffer constructor.
func NewReadBuffer(rd io.Reader, bo binary.ByteOrder) ReadBuffer {
	return ReadBuffer{
		reader: *bufio.NewReader(rd),
		Parser: NewParser(bo),
	}
}

func (rb *ReadBuffer) Peek(n int) ([]byte, error) {
	b, err := rb.reader.Peek(n)
	if err != nil {
		return nil, NewProtocolError(err, "error during buffer peek")
	}

	return b, nil
}

func (rb *ReadBuffer) ReadN(limit int, n int, f func([]byte)) (int, error) {
	if n > limit {
		return limit, ErrNotEnoughBytes
	}

	b, err := rb.reader.Peek(n)
	if err != nil {
		return limit, NewProtocolError(err, "error during buffer peek")
	}

	f(b)

	return rb.DiscardN(limit, n)
}

func (rb *ReadBuffer) DiscardN(limit int, n int) (int, error) {
	var err error
	if n <= limit {
		n, err = rb.reader.Discard(n)
	} else {
		n, err = rb.reader.Discard(limit)
		if err == nil {
			err = ErrNotEnoughBytes
		}
	}

	return limit - n, err
}

func (rb *ReadBuffer) ReadVarString(size uint64) (string, error) {
	buf := make([]byte, size)
	_, err := io.ReadFull(&rb.reader, buf)

	return string(buf), err
}

func (rb *ReadBuffer) ReadMessage(size int64) ([]byte, error) {
	buf := make([]byte, size)
	_, err := io.ReadFull(&rb.reader, buf)
	return buf, err
}

func (rb *ReadBuffer) Remaining() (int, error) {
	remaining, err := rb.reader.Peek(rb.reader.Buffered())
	return len(remaining), err
}

func (rb *ReadBuffer) ReadUint8(limit int, v *uint8) (int, error) {
	return rb.ReadN(limit, SizeOfUint8Bytes, func(b []byte) { *v = rb.Parser.ParseUint8(b) })
}

func (rb *ReadBuffer) ReadUint16(limit int, v *uint16) (int, error) {
	return rb.ReadN(limit, SizeOfUint16Bytes, func(b []byte) { *v = rb.Parser.ParseUint16(b) })
}

func (rb *ReadBuffer) ReadUint32(limit int, v *uint32) (int, error) {
	return rb.ReadN(limit, SizeOfUint32Bytes, func(b []byte) { *v = rb.Parser.ParseUint32(b) })
}

func (rb *ReadBuffer) ReadUint64(limit int, v *uint64) (int, error) {
	return rb.ReadN(limit, SizeOfUint64Bytes, func(b []byte) { *v = rb.Parser.ParseUint64(b) })
}

func (rb *ReadBuffer) Done() bool {
	return rb.reader.Buffered() == 0
}

const (
	eighthBit    = 0x80
	allSevenBits = 0x7f
	errFlags     = 0xfe
)

func (rb *ReadBuffer) ReadVarInt(limit int, v *int64) (remain int, err error) {
	// Optimistically assume that most of the time, there will be data buffered
	// in the reader. If this is not the case, the buffer will be refilled after
	// consuming zero bytes from the input.
	// input, _ := rb.reader.Peek(rb.reader.Buffered())
	// x := uint64(0)
	// s := uint(0)

	// for {
	// 	if len(input) > limit {
	// 		input = input[:limit]
	// 	}

	// 	for i, b := range input {
	// 		if b < eighthBit {
	// 			// x |= uint64(b) << s
	// 			// *v = int64(x>>1) ^ -(int64(x) & 1)
	// 			*v = int64(b)
	// 			n, err := rb.reader.Discard(i + 1)

	// 			return limit - n, err
	// 		}

	// 		x |= uint64(b&allSevenBits) << s
	// 		s += 7
	// 	}

	// 	// Make room in the input buffer to load more data from the underlying
	// 	// stream. The x and s variables are left untouched, ensuring that the
	// 	// varint decoding can continue on the next loop iteration.
	// 	n, _ := rb.reader.Discard(len(input))
	// 	limit -= n

	// 	if limit == 0 {
	// 		return 0, ErrNotEnoughBytes
	// 	}

	// 	// Fill the buffer: ask for one more byte, but in practice the reader
	// 	// will load way more from the underlying stream.
	// 	if _, err := rb.reader.Peek(1); err != nil {
	// 		if err == io.EOF {
	// 			err = ErrNotEnoughBytes
	// 		}

	// 		return limit, err
	// 	}

	// 	// Grab as many bytes as possible from the buffer, then go on to the
	// 	// next loop iteration which is going to consume it.
	// 	input, _ = rb.reader.Peek(rb.reader.Buffered())
	// }

	decoded, err := binary.ReadUvarint(&rb.reader)

	*v = int64(decoded)
	if err != nil {
		return 0, err
	}

	remaining, _ := rb.reader.Peek(rb.reader.Buffered())

	return len(remaining), nil
}
