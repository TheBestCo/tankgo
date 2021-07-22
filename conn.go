package tankgo

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	tbinary "github.com/TheBestCo/tankgo/binary"
)

// Logger interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// Writable interface of every serializable message.
type Writable interface {
	writeToBuffer(c *tbinary.WriteBuffer) error
}

// Readable interface of every serializable message.
type Readable interface {
	readFromBuffer(rb *tbinary.ReadBuffer, payloadSize uint32) error
}

// MessageHandler interface.
type MessageHandler interface {
	OnPing(Conn)
}

// Conn represents a connection to a tank broker.
// Instances of Conn are safe to use concurrently from multiple goroutines.
type Conn struct {
	conn net.Conn
	// read buffer (synchronized on rlock)
	rlock      sync.Mutex
	readBuffer tbinary.ReadBuffer

	// write buffer (synchronized on wlock)
	wlock       sync.Mutex
	writeBuffer tbinary.WriteBuffer

	logger Logger
}

// NewConn returns a new tank connection.
func NewConn(conn net.Conn) *Conn {
	c := &Conn{
		conn:        conn,
		readBuffer:  tbinary.NewReadBuffer(conn, binary.LittleEndian),
		writeBuffer: tbinary.NewWriteBuffer(conn, binary.LittleEndian),
	}

	return c
}

// SetLogger sets logger to connection to print debug logs.
func (c *Conn) SetLogger(logger Logger) {
	c.logger = logger
}

// Send sends a writable message to broker.
func (c *Conn) Send(w Writable) error {
	c.wlock.Lock()
	defer c.wlock.Unlock()

	if err := w.writeToBuffer(&c.writeBuffer); err != nil {
		return tbinary.NewProtocolError(err, "error during write message")
	}

	if err := c.writeBuffer.Flush(); err != nil {
		return tbinary.NewProtocolError(err, "error during write message")
	}

	return nil
}

// ReadMessage blocks an reads (consumes) a message from broker.
func (c *Conn) ReadMessage() (MessageType, interface{}, error) {
	c.rlock.Lock()
	defer c.rlock.Unlock()

	bh := BasicHeader{}
	if err := bh.readFromBuffer(&c.readBuffer, sizeOfBasicHeader); err != nil {
		return messageTypeUnknown, nil, err
	}

	if c.logger != nil {
		// debug log.
		b, _ := c.readBuffer.Peek(int(bh.PayloadSize))
		c.logger.Printf("read message type %d data %#v", bh.MessageType, b)
	}

	msg, err := c.read(bh.MessageType, bh.PayloadSize)

	return bh.MessageType, msg, err
}

func (c *Conn) read(m MessageType, payloadSize uint32) (interface{}, error) {
	var (
		msg interface{}
		err error
	)

	switch m {
	case messageTypeConsume:
		m := ConsumeResponse{}
		err = m.readFromBuffer(&c.readBuffer, payloadSize)
		msg = m

	case messageTypeProduce, messageTypeProduce5:
		m := ProduceResponse{}
		err = m.readFromBuffer(&c.readBuffer, payloadSize)
		msg = m

	case messageTypePing:
		msg = Ping{}

	case messageTypeDiscoverPartitions:
		m := DiscoverPartitionsResponse{}
		err = m.readFromBuffer(&c.readBuffer, payloadSize)
		msg = m

	case messageTypeStatus, messageTypeReloadConf, messageTypeConsumePeer, messageTypeCreateTopic:
		err = tbinary.NewProtocolError(nil, fmt.Sprintf("received message of type %d which is not yet implemented", m))

	case messageTypeUnknown:
		fallthrough
	default:
		err = tbinary.NewProtocolError(nil, fmt.Sprintf("received message of unknown type %d", m))
	}

	return msg, err
}
