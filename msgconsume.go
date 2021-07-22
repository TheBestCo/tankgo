package tankgo

import (
	"github.com/TheBestCo/tankgo/binary"
)

/*
  Fetch Request
  https://github.com/phaistos-networks/TANK/blob/master/tank_protocol.md#fetchreq
*/

// FetchRequestTopicPartition struct.
type FetchRequestTopicPartition struct {
	PartitionID       uint16
	ABSSequenceNumber uint64
	FetchSize         uint32
}

func (FetchRequestTopicPartition) size() uint32 {
	// partition id:u16 + abs. sequence number:u64 + fetch size:u32
	return uint32(binary.SizeOfUint16Bytes + binary.SizeOfUint64Bytes + binary.SizeOfUint32Bytes)
}

func (f *FetchRequestTopicPartition) writeToBuffer(w *binary.WriteBuffer) error {
	// partition id:u16
	if err := w.WriteUint16(f.PartitionID); err != nil {
		return err
	}

	// abs. sequence number: u64
	if err := w.WriteUint64(f.ABSSequenceNumber); err != nil {
		return err
	}

	// fetch size:u32
	return w.WriteUint32(f.FetchSize)
}

// FetchRequestTopic struct.
type FetchRequestTopic struct {
	Name       string
	Partitions []FetchRequestTopicPartition
}

func (f *FetchRequestTopic) size() uint32 {
	var sum uint32
	sum += binary.SizeOfUint8Bytes + uint32(len(f.Name)) // name:str8
	sum += binary.SizeOfUint8Bytes                       // partitions count:u8
	sum += uint32(len(f.Partitions)) * FetchRequestTopicPartition{}.size()

	return sum
}

func (f *FetchRequestTopic) writeToBuffer(w *binary.WriteBuffer) error {
	// name:str8
	if err := w.WriteString8(f.Name); err != nil {
		return err
	}

	// partitions count:u8
	if err := w.WriteUint8(uint8(len(f.Partitions))); err != nil {
		return err
	}

	for i := range f.Partitions {
		if err := f.Partitions[i].writeToBuffer(w); err != nil {
			return err
		}
	}

	return nil
}

// ConsumeRequest message.
type ConsumeRequest struct {
	ClientVersion uint16
	RequestID     uint32
	Client        string
	MaxWaitMS     uint64
	MinBytes      uint32
	Topics        []FetchRequestTopic
}

func (f *ConsumeRequest) size() uint32 {
	var sum uint32

	sum += binary.SizeOfUint16Bytes                        // client version:u16
	sum += binary.SizeOfUint32Bytes                        // request id:u32
	sum += binary.SizeOfUint8Bytes + uint32(len(f.Client)) // client id:str8
	sum += binary.SizeOfUint64Bytes                        // max wait(ms):u64
	sum += binary.SizeOfUint32Bytes                        // min bytes:u32
	sum += binary.SizeOfUint8Bytes                         // topics count:u8

	for i := range f.Topics {
		sum += f.Topics[i].size()
	}

	return sum
}

// writeToBuffer implements theWritable interface.
func (f *ConsumeRequest) writeToBuffer(w *binary.WriteBuffer) error {
	// header
	bh := BasicHeader{
		MessageType: messageTypeConsume,
		PayloadSize: f.size(),
	}

	if err := bh.writeToBuffer(w); err != nil {
		return err
	}

	// client version:u16
	if err := w.WriteUint16(f.ClientVersion); err != nil {
		return err
	}

	// request id:u32
	if err := w.WriteUint32(f.RequestID); err != nil {
		return err
	}

	// client id:str8
	if err := w.WriteString8(f.Client); err != nil {
		return err
	}

	// max wait(ms):u64
	if err := w.WriteUint64(f.MaxWaitMS); err != nil {
		return err
	}

	// min bytes:u32
	if err := w.WriteUint32(f.MinBytes); err != nil {
		return err
	}

	// min bytes:u32
	if err := w.WriteUint8(uint8(len(f.Topics))); err != nil {
		return err
	}

	for i := range f.Topics {
		if err := f.Topics[i].writeToBuffer(w); err != nil {
			return err
		}
	}

	return nil
}

/*
  Fetch Response (0x2)
  https://github.com/phaistos-networks/TANK/blob/master/tank_protocol.md#fetchresp
*/

// ConsumeResponse struct.
type ConsumeResponse struct{}

func (fr *ConsumeResponse) readFromBuffer(rb *binary.ReadBuffer, payloadSize uint32) error {
	limit := int(payloadSize)

	var hl uint32 // header length:u32
	if _, err := rb.ReadUint32(limit, &hl); err != nil {
		return err
	}

	var rid uint32 // request id:u32
	if _, err := rb.ReadUint32(limit, &rid); err != nil {
		return err
	}

	var tc uint8 // topics count:u8
	if _, err := rb.ReadUint8(limit, &tc); err != nil {
		return err
	}

	t := make([]TopicHeader, tc)
	for i := range t {
		err := (&t[i]).readFromBuffer(rb, payloadSize)
		if err != nil {
			return err
		}
	}

	return nil
}

type TopicHeader struct {
	Name string
}

func (fr *TopicHeader) readFromBuffer(rb *binary.ReadBuffer, payloadSize uint32) error {
	limit := int(payloadSize)

	var hl uint32 // header length:u32
	if _, err := rb.ReadUint32(limit, &hl); err != nil {
		return err
	}

	return nil
}
