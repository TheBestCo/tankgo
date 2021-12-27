package binary

import "fmt"

// ProtocolError represents errors during protocol io.
type ProtocolError struct {
	inner error
	msg   string
}

// NewProtocolError constructs a protocol error.
func NewProtocolError(inner error, msg string) *ProtocolError {
	return &ProtocolError{
		inner: inner,
		msg:   msg,
	}
}

// Error function.
func (r *ProtocolError) Error() string {
	if r.inner != nil {
		return fmt.Sprintf("tankgo protocol: %s: %s", r.msg, r.inner.Error())
	}

	return fmt.Sprintf("tankgo protocol: %s", r.msg)
}

// Unwrap function.
func (r *ProtocolError) Unwrap() error {
	return r.inner
}
