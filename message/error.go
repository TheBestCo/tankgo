package message

import "fmt"

type ErrMessage struct {
	inner error
	msg   string
}

// NewMessage constructs a message error.
func NewMessageError(inner error, msg string) *ErrMessage {
	return &ErrMessage{
		inner: inner,
		msg:   msg,
	}
}

// Error function.
func (r *ErrMessage) Error() string {
	if r.inner != nil {
		return fmt.Sprintf("tankgo message error: %s: %s", r.msg, r.inner.Error())
	}

	return fmt.Sprintf("tankgo message: %s", r.msg)
}

// Unwrap function.
func (r *ErrMessage) Unwrap() error {
	return r.inner
}

var EmptyResponse error
