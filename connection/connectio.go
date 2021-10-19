package connection

import (
	"encoding/binary"
	"net"
	"sync"

	tbinary "github.com/TheBestCo/tankgo/binary"
)

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
