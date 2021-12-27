package message

import (
	"errors"
	"fmt"
)

type MessageError struct {
	inner error
	msg   string
}

// NewMessage constructs a message error.
func NewMessageError(inner error, msg string) *MessageError {
	return &MessageError{
		inner: inner,
		msg:   msg,
	}
}

// Error function.
func (r *MessageError) Error() string {
	if r.inner != nil {
		return fmt.Sprintf("tankgo message error: %s: %s", r.msg, r.inner.Error())
	}

	return fmt.Sprintf("tankgo message: %s", r.msg)
}

// Unwrap function.
func (r *MessageError) Unwrap() error {
	return r.inner
}

var (
	ErrHighWaterMarkExceeded = errors.New("high water mark exceeded")
	ErrEmptyResponse         = errors.New("empty tank response")
)
