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
	Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
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
	con *net.TCPConn

	// read buffer (synchronized on rlock)
	rlock      sync.Mutex
	readBuffer tbinary.ReadBuffer

	// write buffer (synchronized on wlock)
	wlock       sync.Mutex
	writeBuffer tbinary.WriteBuffer
}

func NewSubscriber(broker string) (*TankSubscriber, error) {
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
		readBuffer:  tbinary.NewReadBuffer(conn, binary.LittleEndian, 1024),
		writeBuffer: tbinary.NewWriteBuffer(conn, binary.LittleEndian),
	}

	return &t, nil
}

func (s *TankSubscriber) Subscribe(r *message.ConsumeRequest, maxConcurrentReads int) (<-chan message.MessageLog, <-chan error) {

	errChan := make(chan error, 1)
	bh, err := s.sendSubscribeRequest(r)
	if err != nil {
		errChan <- err
		return nil, errChan
	}

	topicPartitionBaseSeq := make(map[string]uint64)

	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			topicPartitionBaseSeq[t.Name+"/"+strconv.Itoa(int(p.PartitionID))] = p.ABSSequenceNumber
		}
	}

	m := message.ConsumeResponse{TopicPartitionBaseSeq: topicPartitionBaseSeq}

	msgChan := make(chan message.MessageLog, maxConcurrentReads)

	// consume from stream in the background.
	done := make(chan bool)
	go func() {
		errChan <- m.Consume(&s.readBuffer, bh.PayloadSize, msgChan)
		done <- true
	}()

	go func() {
		defer close(msgChan)
		<-done
	}()

	return msgChan, errChan
}

func (s *TankSubscriber) Close() error {
	if s.con != nil {
		return s.con.Close()
	}
	return nil
}

// Ping is a wrapper method of readFromTopic expecting a ping response.
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
