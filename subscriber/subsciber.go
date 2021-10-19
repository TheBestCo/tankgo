package subscriber

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"

	tbinary "github.com/TheBestCo/tankgo/binary"
	"github.com/TheBestCo/tankgo/message"
)

type Subscriber interface {
	// Subscribe returns output channel with messages from provided topic.
	// Channel is closed, when Close() was called on the subscriber.
	//
	// To receive the next message, `Ack()` must be called on the received message.
	// If message processing failed and message should be redelivered `Nack()` should be called.
	//
	// When provided ctx is cancelled, subscriber will close subscribe and close output channel.
	// Provided ctx is set to all produced messages.
	// When Nack or Ack is called on the message, context of the message is canceled.
	Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
	// Close closes all subscriptions with their output channels and flush offsets etc. when needed.
	Close() error
}

// Logger interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// Writable interface of every serializable message.
type Writable interface {
	WriteToBuffer(c *tbinary.WriteBuffer) error
}

// Readable interface of every serializable message.
type Readable interface {
	readFromBuffer(rb *tbinary.ReadBuffer, payloadSize uint32) error
}

type TankSubscriber struct {
	broker string
	con    *net.TCPConn

	// read buffer (synchronized on rlock)
	rlock      sync.Mutex
	readBuffer tbinary.ReadBuffer

	// write buffer (synchronized on wlock)
	wlock       sync.Mutex
	writeBuffer tbinary.WriteBuffer
}

func NewSubsriber(broker string) (*TankSubscriber, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", broker)
	if err != nil {
		return &TankSubscriber{}, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return &TankSubscriber{}, err
	}

	t := TankSubscriber{
		con:         conn,
		readBuffer:  tbinary.NewReadBuffer(conn, binary.LittleEndian),
		writeBuffer: tbinary.NewWriteBuffer(conn, binary.LittleEndian),
	}

	return &t, nil
}

func (s *TankSubscriber) Subscribe(r *message.ConsumeRequest, maxConcurrentReads int) (<-chan message.MessageLog, error) {

	bh, err := s.sendSubscribeRequest(r)

	if err != nil {
		return nil, err
	}

	topicPartionBaseSeq := make(map[string]int64)

	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			topicPartionBaseSeq[t.Name+"/"+strconv.Itoa(int(p.PartitionID))] = p.ABSSequenceNumber
		}
	}

	m := message.ConsumeResponse{TopicPartionBaseSeq: topicPartionBaseSeq}

	msgChan := make(chan message.MessageLog, maxConcurrentReads)

	// consume from stream in the background.
	done := make(chan bool)
	go func() {
		err = m.Consume(&s.readBuffer, bh.PayloadSize, msgChan)
		done <- true
		fmt.Println("finished")
	}()

	go func() {
		defer close(msgChan)
		<-done
		fmt.Println("exiting")
	}()

	return msgChan, err
}

// ping is a wrapper method of readFromTopic expecting a ping response.
func (s *TankSubscriber) Ping() error {

	header, err := s.readBasicHeader()

	if header.MessageType != message.TypePing {
		return fmt.Errorf("expected ping response, got :%#v", header.MessageType)
	}
	return err
}

func (s *TankSubscriber) readBasicHeader() (message.BasicHeader, error) {
	s.rlock.Lock()
	defer s.rlock.Unlock()

	bh := message.BasicHeader{}
	if err := bh.ReadHeader(&s.readBuffer, message.SizeOfBasicHeader); err != nil {
		return message.BasicHeader{}, err
	}
	return bh, nil
}

func (s *TankSubscriber) sendSubscribeRequest(w Writable) (message.BasicHeader, error) {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	if err := w.WriteToBuffer(&s.writeBuffer); err != nil {
		return message.BasicHeader{}, tbinary.NewProtocolError(err, "error during write message")
	}

	if err := s.writeBuffer.Flush(); err != nil {
		return message.BasicHeader{}, tbinary.NewProtocolError(err, "error during write message")
	}

	header, err := s.readBasicHeader()

	if err != nil {
		return header, err
	}

	if header.MessageType != message.TypeConsume {
		return header, fmt.Errorf("expected TypeConsume in header message type, got: %#v", header.MessageType)
	}

	return header, nil
}
