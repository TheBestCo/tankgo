package tankgo

import "github.com/TheBestCo/tankgo/binary"

// ProduceRequest struct.
type ProduceRequest struct{}

// ProduceResponse struct.
type ProduceResponse struct{}

// read parses the first 5 bytes into basicHeader.
func (pr *ProduceResponse) readFromBuffer(rb *binary.ReadBuffer, payloadSize uint32) error {
	return nil
}
