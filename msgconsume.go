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
func (f *ConsumeRequest) WriteToBuffer(w *binary.WriteBuffer) error {
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
type ConsumeResponse struct {
	headerLength uint32
	topicHeader  TopicHeader
}

func (fr *ConsumeResponse) readFromBuffer(rb *binary.ReadBuffer, payloadSize uint32) error {
	limit := int(payloadSize)

	var err error

	var hl uint32 // header length:u32
	if _, err = rb.ReadUint32(limit, &hl); err != nil {
		return err
	}
	fr.headerLength = hl

	var rid uint32 // request id:u32
	if _, err = rb.ReadUint32(limit, &rid); err != nil {
		return err
	}
	fr.topicHeader.RequestID = rid

	var tc uint8 // topics count:u8
	if _, err = rb.ReadUint8(limit, &tc); err != nil {
		return err
	}
	fr.topicHeader.TopicsCount = tc

	fr.topicHeader.topics = make([]Topic, fr.topicHeader.TopicsCount)

	for i := range fr.topicHeader.topics {
		err := (fr.topicHeader.topics[i]).readFromTopic(rb, payloadSize)
		if err != nil {
			return err
		}
	}

	return nil
}

type Message struct {
	CreatedAt     uint64
	ContentLength uint64
	SeqNumber     int64
	Data          []byte
}

type Bundle struct {
	BundleFlags uint8
	Messages    []Message
}

type Chunk struct {
	Bundles []Bundle
}

type Partition struct {
	PartitionID   uint16
	ErrorOrFlags  uint8
	HighWaterMark uint64
	ChuckLength   uint32
	Chunk         Chunk
}

type Topic struct {
	Name            string
	TotalPartitions uint8
	Partition       Partition
}

type TopicHeader struct {
	RequestID   uint32
	TopicsCount uint8
	topics      []Topic
}

func (t *Topic) readFromTopic(rb *binary.ReadBuffer, payloadSize uint32) error {
	limit := int(payloadSize)

	var err error

	var ml uint8 // name length:u8
	if _, err = rb.ReadUint8(limit, &ml); err != nil {
		return err
	}

	var name string
	if name, err = rb.ReadVarString(uint64(ml)); err != nil {
		return err
	}

	t.Name = name

	var totalPartitions uint8
	if _, err = rb.ReadUint8(limit, &totalPartitions); err != nil {
		return err
	}
	t.TotalPartitions = totalPartitions

	var partitionID uint16
	if _, err = rb.ReadUint16(limit, &partitionID); err != nil {
		return err
	}

	t.Partition.PartitionID = partitionID

	var errOrFlags uint8
	if _, err := rb.ReadUint8(limit, &errOrFlags); err != nil {
		return err
	}

	t.Partition.ErrorOrFlags = errOrFlags

	if errOrFlags != 0xfe {
		var absBaseSeqNum uint64
		if _, err := rb.ReadUint64(limit, &absBaseSeqNum); err != nil {
			return err
		}
	}

	var highWaterMark uint64
	if _, err := rb.ReadUint64(limit, &highWaterMark); err != nil {
		return err
	}

	t.Partition.HighWaterMark = highWaterMark

	var chunkLength uint32
	if _, err := rb.ReadUint32(limit, &chunkLength); err != nil {
		return err
	}

	t.Partition.ChuckLength = chunkLength

	if errOrFlags == 0x1 {
		var firstAvailSeqNum uint64
		if _, err := rb.ReadUint64(limit, &firstAvailSeqNum); err != nil {
			return err
		}
	}

	messages := make([]Message, 0, 100)
	prevSeqNum := int64(0)
	for !rb.Done() {
		var bundleLength int64
		if _, err = rb.ReadVarInt(limit, &bundleLength); err != nil {
			return err
		}

		var bundleFlag uint8
		if _, err := rb.ReadUint8(limit, &bundleFlag); err != nil {
			return err
		}

		var totalMessages int64
		if bundleFlag>>2&0xf > 15 { // check if total messages in message set > 15
			if _, err := rb.ReadVarInt(limit, &totalMessages); err != nil {
				return err
			}

		} else {
			totalMessages = int64(bundleFlag >> 2 & 0xf)
		}

		sparseBitSet := false
		if bundleFlag&(0x1<<6) > 0 { // check if sparse bit is set
			sparseBitSet = true

		}

		var firstAbsSeqNumber uint64
		var lastAbsSeqNumber int64
		if sparseBitSet {

			if _, err := rb.ReadUint64(limit, &firstAbsSeqNumber); err != nil {
				return err
			}

			if totalMessages > 1 {
				if _, err := rb.ReadVarInt(limit, &lastAbsSeqNumber); err != nil {
					return err
				}

			}
		}

		for i := 0; i < int(totalMessages); i++ {
			var msgFlag uint8
			if _, err := rb.ReadUint8(limit, &msgFlag); err != nil {
				return err
			}

			var messageSequenceNum int64
			if sparseBitSet { // calculate message sequence number
				if msgFlag&0x4 > 0 { //
					messageSequenceNum = prevSeqNum + 1
				} else if i > 0 && i < int(totalMessages)-1 {
					messageSequenceNum++

					var encodedSeqNum int64
					if _, err := rb.ReadVarInt(limit, &encodedSeqNum); err != nil {
						return err
					}

					messageSequenceNum = encodedSeqNum + prevSeqNum + 1
				} else {
					if prevSeqNum == 0 {
						messageSequenceNum = int64(firstAbsSeqNumber)
					} else {
						messageSequenceNum = prevSeqNum + 1
					}
				}
			} else {
				messageSequenceNum = prevSeqNum + 1
			}
			prevSeqNum = messageSequenceNum

			var ts uint64
			usePrevTimestamp := true
			if msgFlag&0x2 == 0 {
				if _, err := rb.ReadUint64(limit, &ts); err != nil {
					return err
				}
				usePrevTimestamp = false
			}

			var msgLength int64
			if _, err := rb.ReadVarInt(limit, &msgLength); err != nil {
				return err
			}

			var message []byte

			if message, err = rb.ReadMessage(msgLength); err != nil {
				return err
			}

			if usePrevTimestamp {
				if len(messages) > 0 {
					ts = messages[i-1].CreatedAt
				}
			}
			messages = append(messages, Message{Data: message, SeqNumber: messageSequenceNum, CreatedAt: ts})
		}
	}

	return nil
}
