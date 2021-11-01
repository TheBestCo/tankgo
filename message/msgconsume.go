package message

import (
	"fmt"
	"strconv"

	"github.com/TheBestCo/tankgo/binary"
)

/*
  Fetch Request
  https://github.com/phaistos-networks/TANK/blob/master/tank_protocol.md#fetchreq
*/

// FetchRequestTopicPartition struct.
type FetchRequestTopicPartition struct {
	PartitionID       int16
	ABSSequenceNumber int64
	FetchSize         int32
}

func (FetchRequestTopicPartition) size() uint32 {
	// partition id:u16 + abs. sequence number:u64 + fetch size:u32
	return uint32(binary.SizeOfUint16Bytes + binary.SizeOfUint64Bytes + binary.SizeOfUint32Bytes)
}

func (f *FetchRequestTopicPartition) writeToBuffer(w *binary.WriteBuffer) error {
	// partition id:u16
	if err := w.WriteUint16(uint16(f.PartitionID)); err != nil {
		return err
	}

	// abs. sequence number: u64
	if err := w.WriteUint64(uint64(f.ABSSequenceNumber)); err != nil {
		return err
	}

	// fetch size:u32
	return w.WriteUint32(uint32(f.FetchSize))
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

// WriteToBuffer implements theWritable interface.
func (f *ConsumeRequest) WriteToBuffer(w *binary.WriteBuffer) error {
	// header
	bh := BasicHeader{
		MessageType: TypeConsume,
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
	headerLength          uint32
	TopicHeader           TopicHeader
	TopicPartitionBaseSeq map[string]int64
}

func (fr *ConsumeResponse) Consume(rb *binary.ReadBuffer, payloadSize uint32, msgLog chan<- MessageLog) error {
	b, _ := rb.Peek(int(payloadSize))

	fmt.Println(b)

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
	fr.TopicHeader.RequestID = rid

	var tc uint8 // topics count:u8
	if _, err = rb.ReadUint8(limit, &tc); err != nil {
		return err
	}
	fr.TopicHeader.TopicsCount = tc

	fr.TopicHeader.Topics = make([]Topic, fr.TopicHeader.TopicsCount)

	for i := range fr.TopicHeader.Topics {
		err := (fr.TopicHeader.Topics[i]).readFromTopic(rb, fr.TopicPartitionBaseSeq, payloadSize, msgLog)
		if err != nil {
			return err
		}
	}

	return nil
}

type Message struct {
	Type MessageType
}

type MessageLog struct {
	CreatedAt uint64
	SeqNumber int64
	Key       []byte
	Payload   []byte
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
	BaseSeqNum      int
}

type TopicHeader struct {
	RequestID   uint32
	TopicsCount uint8
	Topics      []Topic
}

func (t *Topic) readFromTopic(rb *binary.ReadBuffer, topicPartitionBaseSeq map[string]int64, payloadSize uint32, logChan chan<- MessageLog) error {
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

	var absBaseSeqNum uint64
	if errOrFlags != 0xfe {
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

	var firstAvailSeqNum uint64
	if errOrFlags == 0x1 {
		if _, err := rb.ReadUint64(limit, &firstAvailSeqNum); err != nil {
			return err
		}
	}

	baseSeqNumFromReq := topicPartitionBaseSeq[t.Name+"/"+strconv.Itoa(int(t.Partition.PartitionID))]

	prevSeqNum := int64(-1) // when reading a new bundle
	var bundleLength int64 = -10
	var consumedFromBundle int64
	for bundleLength < consumedFromBundle-1 {
		var (
			bundleFlag        uint8
			totalMessages     int64
			firstAbsSeqNumber uint64
			lastAbsSeqNumber  int64
			previousTimestamp uint64
			bundleLimit       int64
		)

		before, _ := rb.Remaining()
		if _, err = rb.ReadVarInt(limit, &bundleLength); err != nil {
			return err
		}
		after, _ := rb.Remaining()
		consumed := before - after
		consumedFromBundle += int64(consumed)

		bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt8Bytes
		if _, err := rb.ReadUint8(limit, &bundleFlag); err != nil {
			if int64(limit) < bundleLimit {
				return err
			} else {
				continue
			}
		}
		consumedFromBundle += binary.SizeOfInt8Bytes

		if (bundleFlag>>2)&0xf == 0 { // check if total messages in message set > 15
			before, _ := rb.Remaining()
			bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt64Bytes
			if _, err := rb.ReadVarInt(limit, &totalMessages); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			after, _ := rb.Remaining()
			consumed := before - after
			bundleLimit += int64(binary.SizeOfInt64Bytes - consumed)
			consumedFromBundle += int64(consumed)

		} else {
			totalMessages = int64((bundleFlag >> 2) & 0xf) // total messages number is encoded in the bundle flag
		}

		sparseBitSet := false
		if bundleFlag&(0x1<<6) > 0 { // check if sparse bit is set
			sparseBitSet = true
		}

		if sparseBitSet {
			bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfUint64Bytes
			if _, err := rb.ReadUint64(int(bundleLimit), &firstAbsSeqNumber); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			consumedFromBundle += binary.SizeOfInt64Bytes

			if totalMessages > 1 {
				before, _ := rb.Remaining()
				bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt64Bytes
				if _, err := rb.ReadVarInt(int(bundleLimit), &lastAbsSeqNumber); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}
				after, _ := rb.Remaining()
				consumed := before - after
				bundleLimit += int64(binary.SizeOfInt64Bytes - consumed)
				consumedFromBundle += int64(consumed)
			}
		}

		var (
			messageSequenceNum int64
			msgFlag            uint8
			ts                 uint64
			msgLength          int64
			message            []byte
			key                []byte
		)
		for i := 0; i < int(totalMessages); i++ {
			bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt8Bytes
			if _, err := rb.ReadUint8(int(bundleLimit), &msgFlag); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			consumedFromBundle += binary.SizeOfUint8Bytes

			if sparseBitSet { // calculate message sequence number
				if msgFlag&0x4 > 0 { //
					messageSequenceNum = prevSeqNum + 1

				} else if i > 0 && i < int(totalMessages)-1 { // this probably never evaluates to true, but if it ever does, then we should probably read a varint here. TODO cross-check with server implementation.
					messageSequenceNum++

					var delta int64
					before, _ := rb.Remaining()
					bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt64Bytes
					if _, err := rb.ReadVarInt(int(bundleLimit), &delta); err != nil {
						if int64(limit) < bundleLimit {
							return err
						} else {
							continue
						}
					}
					after, _ := rb.Remaining()
					consumed := before - after
					bundleLimit += int64(binary.SizeOfInt64Bytes - consumed)
					consumedFromBundle += int64(consumed)

				} else {
					if prevSeqNum == -1 {
						messageSequenceNum = int64(firstAbsSeqNumber)
					} else {
						messageSequenceNum = prevSeqNum + 1
					}
				}
			} else if prevSeqNum == -1 { // this means this is the first message returned from tank server and sparse bit is not set, so it's sequence number is the ABSSequenceNumber we requested.
				messageSequenceNum = baseSeqNumFromReq
			} else {
				messageSequenceNum = prevSeqNum + 1
			}
			prevSeqNum = messageSequenceNum

			usePrevTimestamp := true
			if msgFlag&0x2 == 0 {
				bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfUint64Bytes
				if _, err := rb.ReadUint64(int(bundleLimit), &ts); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}
				consumedFromBundle += binary.SizeOfUint64Bytes

				previousTimestamp = ts
				usePrevTimestamp = false
			}

			if usePrevTimestamp {
				ts = previousTimestamp
			}

			if msgFlag&0x1 > 0 { // check if message has key
				var keyLength uint8
				bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfUint8Bytes
				if _, err = rb.ReadUint8(limit, &keyLength); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}
				consumedFromBundle += binary.SizeOfUint8Bytes

				bundleLimit = bundleLength - consumedFromBundle - int64(keyLength)
				if key, err = rb.ReadMessage(int64(keyLength)); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}
				consumedFromBundle += int64(len(key))
			} else {
				key = nil
			}

			// decode message part
			before, _ := rb.Remaining()
			bundleLimit = bundleLength - consumedFromBundle - binary.SizeOfInt64Bytes
			if _, err := rb.ReadVarInt(int(bundleLimit), &msgLength); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			after, _ := rb.Remaining()
			consumed := before - after
			bundleLimit += int64(binary.SizeOfInt64Bytes - consumed)
			consumedFromBundle += int64(consumed)

			bundleLimit = bundleLength - consumedFromBundle - msgLength
			if message, err = rb.ReadMessage(msgLength); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			consumedFromBundle += int64(len(message))

			// send message to channel. It will block if consumers are reading slower than the client consumes.
			logChan <- MessageLog{Payload: message, SeqNumber: messageSequenceNum, CreatedAt: ts, Key: key}
		}
	}

	return nil
}
