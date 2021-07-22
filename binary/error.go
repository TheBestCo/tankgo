package binary

import "fmt"

// ErrProtocol represents errors during protocol io.
type ErrProtocol struct {
	inner error
	msg   string
}

// NewProtocolError constructs a protocol error.
func NewProtocolError(inner error, msg string) *ErrProtocol {
	return &ErrProtocol{
		inner: inner,
		msg:   msg,
	}
}

// Error function.
func (r *ErrProtocol) Error() string {
	if r.inner != nil {
		return fmt.Sprintf("tankgo protocol: %s: %s", r.msg, r.inner.Error())
	}

	return fmt.Sprintf("tankgo protocol: %s", r.msg)
}

// Unwrap function.
func (r *ErrProtocol) Unwrap() error {
	return r.inner
}
