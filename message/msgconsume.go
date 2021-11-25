package message

import (
	"bytes"
	stdbinary "encoding/binary"
	"fmt"
	"log"
	"strconv"

	"github.com/TheBestCo/tankgo/binary"
	"github.com/golang/snappy"
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
	b, err := rb.Peek(int(payloadSize))
	if err != nil {
		return err
	}

	log.Printf("%+v", b)

	var hl uint32 // header length:u32
	if err = rb.ReadUint32(&hl); err != nil {
		return err
	}
	fr.headerLength = hl

	var rid uint32 // request id:u32
	if err = rb.ReadUint32(&rid); err != nil {
		return err
	}
	fr.TopicHeader.RequestID = rid

	var tc uint8 // topics count:u8
	if err = rb.ReadUint8(&tc); err != nil {
		return err
	}
	fr.TopicHeader.TopicsCount = tc

	fr.TopicHeader.Topics = make([]Topic, fr.TopicHeader.TopicsCount)

	for i := range fr.TopicHeader.Topics {
		err := fr.TopicHeader.Topics[i].readFromTopic(rb, fr.TopicPartitionBaseSeq, payloadSize, msgLog)
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
	SeqNumber uint64
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
	PartitionID      uint16
	ErrorOrFlags     uint8
	HighWaterMark    uint64
	ChuckLength      uint32
	FirstAvailSeqNum uint64
	Chunk            Chunk
}

type Topic struct {
	Name            string
	TotalPartitions uint8
	Partition       Partition
	LogBaseSeqNum   uint64
	MessageEnd      uint64
	BundleEnd       int64
}

type TopicHeader struct {
	RequestID   uint32
	TopicsCount uint8
	Topics      []Topic
}

func (t *Topic) readTopicName(rb *binary.ReadBuffer) error {
	var (
		ml  uint8 // name length:u8
		err error
	)
	if err = rb.ReadUint8(&ml); err != nil {
		return err
	}

	var name string
	if name, err = rb.ReadVarString(uint64(ml)); err != nil {
		return err
	}
	t.Name = name
	return nil
}

func (t *Topic) readPartitionInfo(rb *binary.ReadBuffer) error {
	var (
		totalPartitions uint8
		err             error
	)
	if err = rb.ReadUint8(&totalPartitions); err != nil {
		return err
	}
	t.TotalPartitions = totalPartitions

	var partitionID uint16
	if err = rb.ReadUint16(&partitionID); err != nil {
		return err
	}
	t.Partition.PartitionID = partitionID
	return nil
}

func (t *Topic) readErrorOrFlags(rb *binary.ReadBuffer) error {
	var errOrFlags uint8
	if err := rb.ReadUint8(&errOrFlags); err != nil {
		return err
	}
	t.Partition.ErrorOrFlags = errOrFlags
	return nil
}

func (t *Topic) readLogBaseSeqNumber(rb *binary.ReadBuffer) error {
	var logBaseSeqNumber uint64
	if err := rb.ReadUint64(&logBaseSeqNumber); err != nil {
		return err
	}
	t.LogBaseSeqNum = logBaseSeqNumber
	return nil
}

func (t *Topic) readHighWaterMark(rb *binary.ReadBuffer) error {
	var highWaterMark uint64
	if err := rb.ReadUint64(&highWaterMark); err != nil {
		return err
	}
	t.Partition.HighWaterMark = highWaterMark
	return nil
}

func (t *Topic) readChunkLength(rb *binary.ReadBuffer) error {
	var chunkLength uint32
	if err := rb.ReadUint32(&chunkLength); err != nil {
		return err
	}
	t.Partition.ChuckLength = chunkLength
	return nil
}

func (t *Topic) readFirstAvailableSeqNum(rb *binary.ReadBuffer) error {
	var firstAvailSeqNum uint64
	if t.Partition.ErrorOrFlags == 0x1 {
		if err := rb.ReadUint64(&firstAvailSeqNum); err != nil {
			return err
		}
	}
	//t.Partition.FirstAvailSeqNum = firstAvailSeqNum
	return nil
}

func (t *Topic) getBundleLength(rb *binary.ReadBuffer) (int64, error) {
	var bundleLength int64 = -10
	if _, err := rb.ReadVarUInt(&bundleLength); err != nil {
		return 0, err
	}
	return bundleLength, nil
}

func (t *Topic) getBundleFlag(rb *binary.ReadBuffer) (uint8, error) {
	var bundleFlag uint8
	if err := rb.ReadUint8(&bundleFlag); err != nil {
		return 0, err
	}
	return bundleFlag, nil
}

func (t *Topic) getTotalMessageNum(rb *binary.ReadBuffer, bundleFlag uint8) (int64, error) {
	messageCount := (bundleFlag >> 2) & 0xf
	if messageCount == 0 {
		var totalMessages int64
		if _, err := rb.ReadVarUInt(&totalMessages); err != nil {
			return 0, err
		}
		messageCount = uint8(totalMessages)
	}
	return int64(messageCount), nil
}

func sparseBitSet(bundleFlag uint8) bool {
	return bundleFlag&(0x1<<6) > 0
}

func (t *Topic) getFirstMessageSeqNum(rb *binary.ReadBuffer) (uint64, error) {
	var seqNumber uint64
	if err := rb.ReadUint64(&seqNumber); err != nil {
		return 0, err
	}
	return seqNumber, nil
}

func (t *Topic) getLastMessageOffset(rb *binary.ReadBuffer) (int64, error) {
	var lastMessageOffest int64
	if _, err := rb.ReadVarUInt(&lastMessageOffest); err != nil {
		return 0, err
	}
	return lastMessageOffest, nil
}

func getMessageFlag(rb *binary.ReadBuffer) (uint8, error) {
	var msgFlag uint8
	if err := rb.ReadUint8(&msgFlag); err != nil {
		return 0, err
	}
	return msgFlag, nil
}

func getMsgSequenceDelta(rb *binary.ReadBuffer) (int64, error) {
	var delta int64
	if _, err := rb.ReadVarUInt(&delta); err != nil {
		return 0, err
	}
	return delta, nil
}

func snappyCompressed(bundleFlag uint8) bool {
	return bundleFlag&3 > 0
}

type BundleOffset struct {
	offset int64
}

func (b *BundleOffset) MarkOffset(offset int64) {
	b.offset = offset
}

func (b *BundleOffset) GetOffsetDistance(from int64) int64 {
	return from - b.offset
}

func (t *Topic) readFromTopic(rb *binary.ReadBuffer, topicPartitionBaseSeq map[string]int64, payloadSize uint32, logChan chan<- MessageLog) error {
	limit := int(payloadSize)

	var err error

	if err = t.readTopicName(rb); err != nil {
		return err
	}

	if err = t.readPartitionInfo(rb); err != nil {
		return err
	}

	if err = t.readErrorOrFlags(rb); err != nil {
		return err
	}

	switch t.Partition.ErrorOrFlags {
	case 0xfb:
		return fmt.Errorf("system error while attempting to access topic %s, partition %d", t.Name, t.Partition.PartitionID)
	case 0xff:
		return fmt.Errorf("undefined partion")
	case 0xfd:
		return fmt.Errorf("no current leader for topic %s, partition %d", t.Name, t.Partition.PartitionID)
	case 0xfc: // TODO: maybe this needs more handling
		return fmt.Errorf("different leader")
	case 0xfe: // The first bundle in the bundles chunk is a sparse bundle. It encodes the first and last message seq.number in that bundle header.
		t.LogBaseSeqNum = 0
	default:
		if err = t.readLogBaseSeqNumber(rb); err != nil {
			return err
		}
	}

	if err = t.readHighWaterMark(rb); err != nil {
		return err
	}

	if err = t.readChunkLength(rb); err != nil {
		return err
	}

	if err = t.readFirstAvailableSeqNum(rb); err != nil {
		return err
	}

	bundleLength, err := t.getBundleLength(rb)
	if err != nil {
		return nil // incomplete bundle
	}

	t.BundleEnd = int64(limit-rb.Buffered()) + bundleLength

	bundleOffset := BundleOffset{}
	bundleOffset.MarkOffset(int64(limit - rb.Buffered()))

	for rb.Remaining() > 0 {
		var (
			firstMsgSeqNumber uint64
			lastMsgSeqNumber  uint64
			previousTimestamp uint64
			bundleLimit       int64
		)

		bundleFlag, err := t.getBundleFlag(rb)
		if err != nil {
			return err
		}

		messageCount, err := t.getTotalMessageNum(rb, bundleFlag)
		if err != nil {
			return err
		}

		// check if sparse bit is set
		sparseBundle := sparseBitSet(bundleFlag)
		if sparseBundle {
			var err error
			firstMsgSeqNumber, err = t.getFirstMessageSeqNum(rb)
			if err != nil {
				return err
			}

			if messageCount != 1 {
				lastMessageOffset, err := t.getLastMessageOffset(rb)
				if err != nil {
					return err
				}
				lastMsgSeqNumber = uint64(lastMessageOffset) + firstMsgSeqNumber + 1
			} else {
				lastMsgSeqNumber = firstMsgSeqNumber
			}
			t.LogBaseSeqNum = firstMsgSeqNumber
			t.MessageEnd = lastMsgSeqNumber + 1
		} else {
			t.MessageEnd = t.LogBaseSeqNum + uint64(messageCount)
		} // Finished reading from bundle header.

		requestedSeqNum := topicPartitionBaseSeq[t.Name+"/"+strconv.Itoa(int(t.Partition.PartitionID))]

		if requestedSeqNum > int64(t.MessageEnd) {
			if err = rb.DiscardN(int(t.BundleEnd) - int(bundleOffset.offset)); err != nil {
				return err
			}
			t.LogBaseSeqNum = t.MessageEnd
			continue
		}

		bundleCompressed := snappyCompressed(bundleFlag)
		if bundleCompressed {
			consumedFromBundle := bundleOffset.GetOffsetDistance(int64(limit - rb.Buffered()))
			compressed, err := rb.Peek(int(bundleLength - consumedFromBundle))

			if err != nil {
				return err
			}
			decoded, err := snappy.Decode(nil, compressed)
			if err != nil {
				return err
			}
			//rb.DiscardN(len(compressed))
			fmt.Println(decoded)
			*rb = binary.NewReadBuffer(bytes.NewReader(decoded), stdbinary.LittleEndian)
			bundleLength = int64(len(decoded))

		}

		var (
			ts        uint64
			msgLength int64
			message   []byte
			key       []byte
		)

		//messageSequenceNum := int64(firstMsgSeqNumber)
		for i := 0; i < int(messageCount); i++ {
			msgFlag, err := getMessageFlag(rb)
			if err != nil {
				return err
			}

			if sparseBundle { // calculate message sequence number
				switch {
				case i == 0:
					t.LogBaseSeqNum = firstMsgSeqNumber
				case msgFlag&0x4 > 0:
					break
				case i == int(messageCount)-1:
					if err != nil {
						return err
					}
					t.LogBaseSeqNum = lastMsgSeqNumber
					//messageSequenceNum = delta + prevSeqNum + 1
				default:
					delta, err := getMsgSequenceDelta(rb)
					if err != nil {
						return err
					}
					t.LogBaseSeqNum += uint64(delta)
					//messageSequenceNum = int64(lastMsgSeqNumber)
				}
				t.LogBaseSeqNum++
				//prevSeqNum = messageSequenceNum
			}

			messageSequenceNum := t.LogBaseSeqNum

			usePrevTimestamp := true
			if msgFlag&0x2 == 0 {
				if err := rb.ReadUint64(&ts); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}

				previousTimestamp = ts
				usePrevTimestamp = false
			}

			if usePrevTimestamp {
				ts = previousTimestamp
			}

			if msgFlag&0x1 > 0 { // check if message has key
				var keyLength uint8
				if err = rb.ReadUint8(&keyLength); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}

				if key, err = rb.ReadMessage(int64(keyLength)); err != nil {
					if int64(limit) < bundleLimit {
						return err
					} else {
						continue
					}
				}
			} else {
				key = nil
			}

			// decode message part
			before := rb.Remaining()
			if _, err := rb.ReadVarUInt(&msgLength); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}
			after := rb.Remaining()
			consumed := before - after
			bundleLimit += int64(binary.SizeOfInt64Bytes - consumed)

			if message, err = rb.ReadMessage(msgLength); err != nil {
				if int64(limit) < bundleLimit {
					return err
				} else {
					continue
				}
			}

			// send message to channel. It will block if consumers are reading slower than the client's send rate.
			logChan <- MessageLog{Payload: message, SeqNumber: messageSequenceNum, CreatedAt: ts, Key: key}
		}
	}

	return nil
}
