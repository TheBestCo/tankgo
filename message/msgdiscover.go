package message

import "github.com/TheBestCo/tankgo/binary"

// DiscoverPartitionsRequest struct.
type DiscoverPartitionsRequest struct{}

// DiscoverPartitionsResponse struct.
type DiscoverPartitionsResponse struct{}

// read parses the first 5 bytes into basicHeader.
func (bh *DiscoverPartitionsResponse) ReadFromBuffer(rb *binary.ReadBuffer, payloadSize uint32) error {
	return nil
}
