package binary

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

/*
 *  Protocol write functionality
 */

// WriteBuffer struct.
type WriteBuffer struct {
	Writer           bufio.Writer
	b                [16]byte
	binaryEndianness binary.ByteOrder
}

// NewWriteBuffer constructor.
func NewWriteBuffer(w io.Writer, binaryEndianness binary.ByteOrder) WriteBuffer {
	return WriteBuffer{
		Writer:           *bufio.NewWriter(w),
		binaryEndianness: binaryEndianness,
	}
}

//
// bufio.Writer wrappers
//

func (wb *WriteBuffer) Write(b []byte) (int, error) {
	n, err := wb.Writer.Write(b)
	if err != nil {
		return n, NewProtocolError(err, "error during write buffer write")
	}

	return n, nil
}

func (wb *WriteBuffer) Flush() error {
	if err := wb.Writer.Flush(); err != nil {
		return NewProtocolError(err, "error during write buffer flush")
	}

	return nil
}

//
// Internal
//

func (wb *WriteBuffer) WriteUint8(i uint8) error {
	wb.b[0] = i

	n, err := wb.Write(wb.b[:SizeOfUint8Bytes])
	if err != nil {
		return err
	}

	if n != SizeOfUint8Bytes {
		return NewProtocolError(nil, fmt.Sprintf("writer error, expected to write %d wrote %d", SizeOfUint8Bytes, n))
	}

	return nil
}

func (wb *WriteBuffer) WriteUint16(i uint16) error {
	wb.binaryEndianness.PutUint16(wb.b[:SizeOfUint16Bytes], i)

	n, err := wb.Write(wb.b[:SizeOfUint16Bytes])
	if err != nil {
		return err
	}

	if n != SizeOfUint16Bytes {
		return NewProtocolError(nil, fmt.Sprintf("writer error, expected to write %d wrote %d", SizeOfUint16Bytes, n))
	}

	return nil
}

func (wb *WriteBuffer) WriteUint32(i uint32) error {
	wb.binaryEndianness.PutUint32(wb.b[:SizeOfUint32Bytes], i)

	n, err := wb.Write(wb.b[:SizeOfUint32Bytes])
	if err != nil {
		return err
	}

	if n != SizeOfUint32Bytes {
		return NewProtocolError(nil, fmt.Sprintf("writer error, expected to write %d wrote %d", SizeOfUint32Bytes, n))
	}

	return nil
}

func (wb *WriteBuffer) WriteUint64(i uint64) error {
	wb.binaryEndianness.PutUint64(wb.b[:SizeOfUint64Bytes], i)

	n, err := wb.Write(wb.b[:SizeOfUint64Bytes])
	if err != nil {
		return err
	}

	if n != SizeOfUint64Bytes {
		return NewProtocolError(nil, fmt.Sprintf("writer error, expected to write %d wrote %d", SizeOfUint64Bytes, n))
	}

	return nil
}

func (wb *WriteBuffer) WriteString8(s string) error {
	err := wb.WriteUint8(uint8(len(s)))
	if err != nil {
		return err
	}

	return wb.WriteString(s)
}

func (wb *WriteBuffer) WriteString(s string) error {
	_, err := io.WriteString(&wb.Writer, s)
	if err != nil {
		return NewProtocolError(nil, "writer write string error")
	}

	return nil
}

func (wb *WriteBuffer) Reset(w io.Writer) {
	wb.b = [16]byte{}
	wb.Writer.Reset(w)
}
