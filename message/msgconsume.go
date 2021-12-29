package message

import (
	"bufio"
	"bytes"
	stdbinary "encoding/binary"
	"errors"
	"fmt"
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
	ABSSequenceNumber uint64
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
	if err := w.WriteUint64(f.ABSSequenceNumber); err != nil {
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
	TopicPartitionBaseSeq map[string]uint64
}

func (fr *ConsumeResponse) Consume(rb *binary.ReadBuffer, payloadSize uint32, msgLog chan<- Log) error {
	var hl uint32 // header length:u32
	if err := rb.ReadUint32(&hl); err != nil {
		return err
	}

	fr.headerLength = hl

	var rid uint32 // request id:u32
	if err := rb.ReadUint32(&rid); err != nil {
		return err
	}

	fr.TopicHeader.RequestID = rid

	var tc uint8 // topics count:u8
	if err := rb.ReadUint8(&tc); err != nil {
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
func (fr *ConsumeResponse) GetTopicsLatestSequenceNumber(rb *binary.ReadBuffer) (map[string]uint64, error) {
	var hl uint32 // header length:u32
	if err := rb.ReadUint32(&hl); err != nil {
		return map[string]uint64{}, err
	}

	fr.headerLength = hl

	var rid uint32 // request id:u32
	if err := rb.ReadUint32(&rid); err != nil {
		return map[string]uint64{}, err
	}

	fr.TopicHeader.RequestID = rid

	var tc uint8 // topics count:u8
	if err := rb.ReadUint8(&tc); err != nil {
		return map[string]uint64{}, err
	}

	fr.TopicHeader.TopicsCount = tc

	fr.TopicHeader.Topics = make([]Topic, fr.TopicHeader.TopicsCount)

	topipHighWaterMarkMap := make(map[string]uint64)

	for i := range fr.TopicHeader.Topics {
		highWaterMark, err := fr.TopicHeader.Topics[i].getHighWaterMark(rb)
		if err != nil {
			return map[string]uint64{}, err
		}

		topipHighWaterMarkMap[fr.TopicHeader.Topics[i].Name] = highWaterMark
	}

	return topipHighWaterMarkMap, nil
}

type Message struct {
	Type Type
}

type Log struct {
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
	Bundles            []Bundle
	ConsumedFromBundle uint64
}

type Partition struct {
	PartitionID       uint16
	ErrorOrFlags      uint8
	HighWaterMark     uint64
	ChuckLength       uint32
	FirstAvailSeqNum  uint64
	Chunk             Chunk
	ConsumedFromChunk uint64
}

type Topic struct {
	Name            string
	TotalPartitions uint8
	Partition       Partition
	LogBaseSeqNum   uint64
	MessageEnd      uint64
}

type TopicHeader struct {
	RequestID   uint32
	TopicsCount uint8
	Topics      []Topic
}

func (t *Topic) readTopicName(rb *binary.ReadBuffer) error {
	var (
		ml   uint8 // name length:u8
		err  error
		name string
	)

	if err = rb.ReadUint8(&ml); err != nil {
		return err
	}

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

	return nil
}

func (t *Topic) getBundleLength(rb *binary.ReadBuffer) (uint64, error) {
	var bundleLength uint64
	if _, err := rb.ReadVarUInt(&bundleLength); err != nil {
		return 0, err
	}

	bs := make([]byte, 10)
	stdbinary.LittleEndian.PutUint64(bs, bundleLength)
	size := stdbinary.PutUvarint(bs, bundleLength)

	if size <= 0 {
		return 0, fmt.Errorf("cannot count varint size")
	}

	t.Partition.ConsumedFromChunk += uint64(size)

	return bundleLength, nil
}

func (t *Topic) getBundleFlag(rb *binary.ReadBuffer) (uint8, error) {
	var bundleFlag uint8
	if err := rb.ReadUint8(&bundleFlag); err != nil {
		return 0, err
	}

	t.Partition.Chunk.ConsumedFromBundle += binary.SizeOfUint8Bytes

	return bundleFlag, nil
}

func (t *Topic) getTotalMessageNum(rb *binary.ReadBuffer, bundleFlag uint8) (int, error) {
	messageCount := (bundleFlag >> 2) & 0xf
	if messageCount == 0 {
		var totalMessages uint64
		if _, err := rb.ReadVarUInt(&totalMessages); err != nil {
			return 0, err
		}

		messageCount = uint8(totalMessages)
		bs := make([]byte, 10)
		stdbinary.LittleEndian.PutUint64(bs, uint64(messageCount))
		size := stdbinary.PutUvarint(bs, uint64(messageCount))

		if size <= 0 {
			return 0, fmt.Errorf("cannot count varint size")
		}

		t.Partition.Chunk.ConsumedFromBundle += uint64(size)
	}

	return int(messageCount), nil
}

func sparseBitSet(bundleFlag uint8) bool {
	return bundleFlag&(0x1<<6) > 0
}

func readNewTimestamp(msgFlag uint8) bool {
	return msgFlag&0x2 == 0
}

func (t *Topic) getFirstMessageSeqNum(rb *binary.ReadBuffer) (uint64, error) {
	var seqNumber uint64
	if err := rb.ReadUint64(&seqNumber); err != nil {
		return 0, err
	}

	bs := make([]byte, 10)
	stdbinary.LittleEndian.PutUint64(bs, seqNumber)
	size := stdbinary.PutUvarint(bs, seqNumber)

	if size <= 0 {
		return 0, fmt.Errorf("cannot count varint size")
	}

	t.Partition.Chunk.ConsumedFromBundle += uint64(size)

	return seqNumber, nil
}

func (t *Topic) getLastMessageOffset(rb *binary.ReadBuffer) (uint64, error) {
	var lastMessageOffest uint64
	if _, err := rb.ReadVarUInt(&lastMessageOffest); err != nil {
		return 0, err
	}

	bs := make([]byte, 10)
	stdbinary.LittleEndian.PutUint64(bs, lastMessageOffest)
	size := stdbinary.PutUvarint(bs, lastMessageOffest)

	if size <= 0 {
		return 0, fmt.Errorf("cannot count varint size")
	}

	t.Partition.Chunk.ConsumedFromBundle += uint64(size)

	return lastMessageOffest, nil
}

func (t *Topic) getRequestedSeqNum(topicPartitionBaseSeq map[string]uint64) uint64 {
	return topicPartitionBaseSeq[t.Name+"/"+strconv.Itoa(int(t.Partition.PartitionID))]
}

func (t *Topic) advanceBaseSeqNum(rb *binary.ReadBuffer, index, messageCount int, firstMsgSeqNumber, lastMsgSeqNumber uint64, msgFlag uint8) error {
	switch {
	case index == 0:
		t.LogBaseSeqNum = firstMsgSeqNumber
	case msgFlag&0x4 > 0:
		break
	case index == messageCount-1:
		t.LogBaseSeqNum = lastMsgSeqNumber
	default:
		delta, err := getMsgSequenceDelta(rb)
		if err != nil {
			return err
		}

		t.LogBaseSeqNum += delta
	}

	return nil
}

func (t *Topic) advanceConsumedFromChunk(size uint64) {
	t.Partition.ConsumedFromChunk += size
}

func (t *Topic) resetConsumedFromBundle() {
	t.Partition.Chunk.ConsumedFromBundle = 0
}

func getMessageFlag(rb *binary.ReadBuffer) (uint8, error) {
	var msgFlag uint8
	if err := rb.ReadUint8(&msgFlag); err != nil {
		return 0, err
	}

	return msgFlag, nil
}

func getMsgSequenceDelta(rb *binary.ReadBuffer) (uint64, error) {
	var delta uint64
	if _, err := rb.ReadVarUInt(&delta); err != nil {
		return 0, err
	}

	return delta, nil
}

func readTimestamp(rb *binary.ReadBuffer) (uint64, error) {
	var ts uint64
	if err := rb.ReadUint64(&ts); err != nil {
		return 0, err
	}

	return ts, nil
}

func readMessageKey(rb *binary.ReadBuffer) ([]byte, error) {
	var (
		keyLength uint8
		keyData   []byte
		err       error
	)

	if err = rb.ReadUint8(&keyLength); err != nil {
		return []byte{}, err
	}

	if keyData, err = rb.ReadMessage(int64(keyLength)); err != nil {
		return []byte{}, err
	}

	return keyData, nil
}

func readMessagePayload(rb *binary.ReadBuffer) ([]byte, error) {
	var (
		msgLength uint64
		payload   []byte
		err       error
	)

	if _, err = rb.ReadVarUInt(&msgLength); err != nil {
		return []byte{}, err
	}

	if payload, err = rb.ReadMessage(int64(msgLength)); err != nil {
		return []byte{}, err
	}

	return payload, nil
}

func getCompressedData(rb *binary.ReadBuffer, length int) ([]byte, error) {
	compressed := make([]byte, 0, length)
	read := 0
	remaining := length

	for read < length {
		if rb.Buffered() > 0 && rb.Buffered() < remaining {
			remaining = rb.Buffered()
		} else {
			remaining = length - read
		}

		b, err := rb.Peek(remaining)

		if err != nil {
			unwrapped := errors.Unwrap(err)
			if errors.As(unwrapped, &bufio.ErrBufferFull) {
				compressed = append(compressed, b...)
				read += len(b)
				rb.DiscardN(len(b))

				continue
			}

			return []byte{}, err
		}

		compressed = append(compressed, b...)
		read += len(b)
		rb.DiscardN(len(b))
	}

	return compressed, nil
}

func snappyCompressed(bundleFlag uint8) bool {
	return bundleFlag&3 == 1
}

func (t *Topic) NextBundle(rb *binary.ReadBuffer) bool {
	return t.Partition.ConsumedFromChunk < uint64(t.Partition.ChuckLength)
}

func (t *Topic) readMessages(rb *binary.ReadBuffer, sparseBundle bool, messageCount int, firstMsgSeqNumber, lastMsgSeqNumber, requestedSeqNum uint64, logChan chan<- Log) error {
	var ts uint64

	for i := 0; i < messageCount; i++ {
		msgFlag, err := getMessageFlag(rb)
		if err != nil {
			return NewMessageError(err, "error while reading message flag")
		}

		// calculate base sequence number for a sparse bundle
		if sparseBundle {
			if err = t.advanceBaseSeqNum(rb, i, messageCount, firstMsgSeqNumber, lastMsgSeqNumber, msgFlag); err != nil {
				return err
			}
		}

		messageSequenceNum := t.LogBaseSeqNum

		if readNewTimestamp(msgFlag) {
			var err error
			ts, err = readTimestamp(rb)

			if err != nil {
				return NewMessageError(err, "error while reading message timestamp")
			}
		}

		var key []byte

		messageHasKey := msgFlag&0x1 == 1

		// decode key part
		if messageHasKey {
			key, err = readMessageKey(rb)
			if err != nil {
				return NewMessageError(err, "error while reading message key")
			}
		}

		// decode message part
		messagePayload, err := readMessagePayload(rb)
		if err != nil {
			return NewMessageError(err, "error while reading message data")
		}

		// send message to channel. It will block if consumers are reading slower than the client's send rate.
		if messageSequenceNum >= requestedSeqNum {
			logChan <- Log{Payload: messagePayload, SeqNumber: messageSequenceNum, CreatedAt: ts, Key: key}
		}

		t.LogBaseSeqNum++
	}

	return nil
}

func (t *Topic) getHighWaterMark(rb *binary.ReadBuffer) (uint64, error) {
	var (
		err error
	)

	if err = t.readTopicName(rb); err != nil {
		return 0, NewMessageError(err, "error while reading topic name")
	}

	if err = t.readPartitionInfo(rb); err != nil {
		return 0, NewMessageError(err, "error while reading partition info")
	}

	if rb.Remaining() == 0 {
		return 0, NewMessageError(ErrEmptyResponse, "empty tank response")
	}

	if err = t.readErrorOrFlags(rb); err != nil {
		return 0, NewMessageError(err, "error while reading errorOrFlags")
	}

	switch t.Partition.ErrorOrFlags {
	case 0xfb:
		errMsg := fmt.Sprintf("system error while attempting to access topic %s, partition %d", t.Name, t.Partition.PartitionID)

		return 0, NewMessageError(nil, errMsg)
	case 0xff:
		return 0, NewMessageError(nil, "undefined partion")
	case 0xfd:
		errMsg := fmt.Sprintf("no current leader for topic %s, partition %d", t.Name, t.Partition.PartitionID)

		return 0, NewMessageError(nil, errMsg)
	case 0xfc: // TODO: maybe this needs more handling
		return 0, NewMessageError(nil, "different leader")
	case 0xfe: // The first bundle in the bundles chunk is a sparse bundle. It encodes the first and last message seq.number in that bundle header.
		t.LogBaseSeqNum = 0
	default:
		if err = t.readLogBaseSeqNumber(rb); err != nil {
			return 0, NewMessageError(err, "error in partition flags")
		}
	}

	if err = t.readHighWaterMark(rb); err != nil {
		return 0, NewMessageError(err, "error while reading high water mark")
	}

	return t.Partition.HighWaterMark, nil
}

func (t *Topic) readFromTopic(rb *binary.ReadBuffer, topicPartitionBaseSeq map[string]uint64, payloadSize uint32, logChan chan<- Log) error {
	var (
		err error
	)

	if err = t.readTopicName(rb); err != nil {
		return NewMessageError(err, "error while reading topic name")
	}

	if err = t.readPartitionInfo(rb); err != nil {
		return NewMessageError(err, "error while reading partition info")
	}

	if rb.Remaining() == 0 {
		return NewMessageError(ErrEmptyResponse, "empty tank response while reading from topic")
	}

	if err = t.readErrorOrFlags(rb); err != nil {
		return NewMessageError(err, "error in partition flags")
	}

	switch t.Partition.ErrorOrFlags {
	case 0xfb:
		errMsg := fmt.Sprintf("system error while attempting to access topic %s, partition %d", t.Name, t.Partition.PartitionID)

		return NewMessageError(nil, errMsg)
	case 0xff:
		return NewMessageError(nil, "undefined partion")
	case 0xfd:
		errMsg := fmt.Sprintf("no current leader for topic %s, partition %d", t.Name, t.Partition.PartitionID)

		return NewMessageError(nil, errMsg)
	case 0xfc: // TODO: maybe this needs more handling
		return NewMessageError(nil, "different leader")
	case 0xfe: // The first bundle in the bundles chunk is a sparse bundle. It encodes the first and last message seq.number in that bundle header.
		t.LogBaseSeqNum = 0
	default:
		if err = t.readLogBaseSeqNumber(rb); err != nil {
			return NewMessageError(err, "error in partition flags")
		}
	}

	if err = t.readHighWaterMark(rb); err != nil {
		return NewMessageError(err, "error while reading high water mark")
	}

	requestedSeqNum := t.getRequestedSeqNum(topicPartitionBaseSeq)

	if requestedSeqNum > t.Partition.HighWaterMark {
		errMsg := fmt.Sprintf("requested sequence number cannot be greater than last committed message sequence number: %d", t.Partition.HighWaterMark)

		return NewMessageError(ErrHighWaterMarkExceeded, errMsg)
	}

	if err = t.readChunkLength(rb); err != nil {
		return NewMessageError(err, "error while reading chunck length")
	}

	if err = t.readFirstAvailableSeqNum(rb); err != nil {
		return NewMessageError(err, "error while reading first available sequence number")
	}

	for t.NextBundle(rb) {
		bundleLength, err := t.getBundleLength(rb)
		if err != nil || bundleLength == 0 {
			return NewMessageError(err, "incomplete bundle") // incomplete bundle
		}

		var (
			firstMsgSeqNumber uint64
			lastMsgSeqNumber  uint64
		)

		bundleFlag, err := t.getBundleFlag(rb)
		if err != nil {
			return NewMessageError(err, "error while reading bundle flag")
		}

		messageCount, err := t.getTotalMessageNum(rb, bundleFlag)
		if err != nil {
			return NewMessageError(err, "error while reading bundle message count")
		}

		// check if sparse bit is set
		sparseBundle := sparseBitSet(bundleFlag)
		if sparseBundle {
			var err error
			firstMsgSeqNumber, err = t.getFirstMessageSeqNum(rb)

			if err != nil {
				return NewMessageError(err, "error while reading sparse bit")
			}

			if messageCount != 1 {
				lastMessageOffset, err := t.getLastMessageOffset(rb)
				if err != nil {
					return NewMessageError(err, "error while reading last message offset")
				}

				lastMsgSeqNumber = lastMessageOffset + firstMsgSeqNumber + 1
			} else {
				lastMsgSeqNumber = firstMsgSeqNumber
			}

			t.LogBaseSeqNum = firstMsgSeqNumber
			t.MessageEnd = lastMsgSeqNumber + 1
		} else {
			t.MessageEnd = t.LogBaseSeqNum + uint64(messageCount)
		} // Finished reading from bundle header.

		bundleCompressed := snappyCompressed(bundleFlag)

		if bundleCompressed {
			compressed, err := getCompressedData(rb, int(bundleLength-t.Partition.Chunk.ConsumedFromBundle))
			if err != nil {
				return NewMessageError(err, "error while retrieving compressed bundle data")
			}

			decoded, err := snappy.Decode(nil, compressed)

			if err != nil {
				return NewMessageError(err, "error in snappy decoding")
			}

			snappyReader := binary.NewReadBuffer(bytes.NewReader(decoded), stdbinary.LittleEndian, len(decoded))
			t.readMessages(&snappyReader, sparseBundle, messageCount, firstMsgSeqNumber, lastMsgSeqNumber, requestedSeqNum, logChan)
		} else {
			t.readMessages(rb, sparseBundle, messageCount, firstMsgSeqNumber, lastMsgSeqNumber, requestedSeqNum, logChan)
		}

		t.advanceConsumedFromChunk(bundleLength)
		t.resetConsumedFromBundle()
	}

	return nil
}
