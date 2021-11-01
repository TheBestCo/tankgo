package message

import (
	"github.com/TheBestCo/tankgo/binary"
)

// SizeOfBasicHeader =  msgId:u8 + payload size:u32 .
const SizeOfBasicHeader = binary.SizeOfUint8Bytes + binary.SizeOfUint32Bytes

// BasicHeader struct.
type BasicHeader struct {
	MessageType MessageType
	PayloadSize uint32
}

// writeToBuffer implements theWritable interface.
func (b BasicHeader) writeToBuffer(w *binary.WriteBuffer) error {
	if err := w.WriteUint8(uint8(b.MessageType)); err != nil {
		return err
	}

	return w.WriteUint32(b.PayloadSize)
}

// ParseBasicHeader parses a basic header from a slice.
func ParseBasicHeader(prs binary.Parser, b []byte) (BasicHeader, error) {
	_ = b[SizeOfBasicHeader-1] // bounds check hint to compiler

	if len(b) < SizeOfBasicHeader {
		return BasicHeader{}, binary.ErrNotEnoughBytes
	}

	return BasicHeader{
		MessageType: MessageType(prs.ParseUint8(b[:binary.SizeOfUint8Bytes])),
		PayloadSize: prs.ParseUint32(b[binary.SizeOfUint8Bytes:]),
	}, nil
}

// PeakBasicHeader parses the first 5 bytes into basicHeader without discarding the bytes from read buffer.
func PeakBasicHeader(rb *binary.ReadBuffer) (BasicHeader, error) {
	b, err := rb.Peek(SizeOfBasicHeader)
	if err != nil {
		return BasicHeader{}, err
	}

	return ParseBasicHeader(rb.Parser, b)
}

// ReadHeader parses the first 5 bytes into basicHeader.
func (b *BasicHeader) ReadHeader(rb *binary.ReadBuffer, payloadSize uint32) error {
	var mt uint8

	if _, err := rb.ReadUint8(binary.SizeOfUint8Bytes, &mt); err != nil {
		return err
	}

	var payload uint32
	if _, err := rb.ReadUint32(binary.SizeOfUint32Bytes, &payload); err != nil {
		return err
	}

	b.MessageType = MessageType(mt)
	b.PayloadSize = payload

	return nil
}
