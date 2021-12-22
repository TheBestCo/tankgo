package binary

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
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
func NewReadBuffer(rd io.Reader, bo binary.ByteOrder, size int) ReadBuffer {
	return ReadBuffer{
		reader: *bufio.NewReaderSize(rd, size),
		Parser: NewParser(bo),
	}
}

func (rb *ReadBuffer) Peek(n int) ([]byte, error) {
	b, err := rb.reader.Peek(n)
	if err != nil {
		return b, NewProtocolError(err, "error during buffer peek")
	}

	return b, nil
}

func (rb *ReadBuffer) ReadN(n int, f func([]byte)) error {
	b, err := rb.reader.Peek(n)
	if err != nil {
		return NewProtocolError(err, "error during buffer peek")
	}

	f(b)

	return rb.DiscardN(n)
}

func (rb *ReadBuffer) DiscardN(n int) error {
	var err error
	if n <= rb.Remaining() {
		_, err = rb.reader.Discard(n)
	} else {
		_, err = rb.reader.Discard(rb.Buffered())
		if err == nil {
			err = ErrNotEnoughBytes
		}
	}

	return err
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

func (rb *ReadBuffer) Buffered() int {
	return rb.reader.Buffered()
}

func (rb *ReadBuffer) Remaining() int {
	return rb.reader.Buffered()
}

func (rb *ReadBuffer) ReadUint8(v *uint8) error {
	return rb.ReadN(SizeOfUint8Bytes, func(b []byte) { *v = rb.Parser.ParseUint8(b) })
}

func (rb *ReadBuffer) ReadUint16(v *uint16) error {
	return rb.ReadN(SizeOfUint16Bytes, func(b []byte) { *v = rb.Parser.ParseUint16(b) })
}

func (rb *ReadBuffer) ReadUint32(v *uint32) error {
	return rb.ReadN(SizeOfUint32Bytes, func(b []byte) { *v = rb.Parser.ParseUint32(b) })
}

func (rb *ReadBuffer) ReadUint64(v *uint64) error {
	return rb.ReadN(SizeOfUint64Bytes, func(b []byte) { *v = rb.Parser.ParseUint64(b) })
}

func (rb *ReadBuffer) Done() bool {
	return rb.reader.Buffered() == 0
}

func (rb *ReadBuffer) SnappyReader() *snappy.Reader {
	return snappy.NewReader(&rb.reader)
}

func (rb *ReadBuffer) ReadVarUInt(v *uint64) (remain int, err error) {
	decoded, err := binary.ReadUvarint(&rb.reader)
	*v = decoded
	if err != nil {
		return 0, err
	}

	return rb.Remaining(), nil
}

func (rb *ReadBuffer) Reset(r io.Reader) {
	rb.reader.Reset(r)
}
