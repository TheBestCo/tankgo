package binary

import (
	"bufio"
	"encoding/binary"
	"fmt"
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

	if n != len(b) {
		fmt.Print("more")
	}
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

const (
	eighthBit    = 0x80
	allSevenBits = 0x7f
	errFlags     = 0xfe
)

func (rb *ReadBuffer) ReadVarUInt(v *uint64) (remain int, err error) {
	decoded, err := binary.ReadUvarint(&rb.reader)
	*v = decoded
	if err != nil {
		return 0, err
	}

	return rb.Remaining(), nil
}

func NewBuffer() *Buffer {
	return &Buffer{
		data: make([]byte, 1024),
	}
}

type Buffer struct {
	data []byte
	i    int
	r    io.Reader
	err  error
}

func (b *Buffer) Reset(r io.Reader) {
	b.data = b.data[:0]
	b.i = 0
	b.err = nil
	b.r = r
}

func (b *Buffer) Next(l int) ([]byte, error) {
	if b.i+l > len(b.data) {
		// Asking for more data than we have. refill
		if err := b.refill(l); err != nil {
			return nil, err
		}
	}

	b.i += l
	return b.data[b.i-l : b.i], nil
}

// Peek allows direct access to the current remaining buffer
func (b *Buffer) Peek() []byte {
	return b.data[b.i:]
}

// Dicard consumes data in the current buffer
func (b *Buffer) Discard(n int) {
	b.i += n
}

// Refill forces the buffer to try to put at least one more byte into its buffer
func (b *Buffer) Refill() error {
	return b.refill(1)
}

func (b *Buffer) refill(l int) error {
	if b.err != nil {
		// We already know we can't get more data
		return b.err
	}

	// fill the rest of the buffer from the reader
	if b.r != nil {
		// shift existing data down over the read portion of the buffer
		n := copy(b.data[:cap(b.data)], b.data[b.i:])
		b.i = 0

		read, err := io.ReadFull(b.r, b.data[n:cap(b.data)])

		b.data = b.data[:n+read]
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		b.err = err
	}

	if b.i+l > len(b.data) {
		// Still not enough data
		return io.ErrUnexpectedEOF
	}

	return nil
}
