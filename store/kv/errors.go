package kv

import "fmt"

type StoreErrCode byte

const (
	ErrCodeInvalid      StoreErrCode = 0x00
	ErrCodeDuplicateKey StoreErrCode = 0x01
)

var (
	// ErrDuplicateKey is returned when an insert call is made for a key that already exist.
	ErrDuplicateKey = NewStoreError(ErrCodeDuplicateKey, "duplicate key value, violates key constraint")
)

type StoreError struct {
	code    StoreErrCode
	msg     string
	wrapped error
}

func NewStoreError(code StoreErrCode, msg string, args ...interface{}) error {
	return StoreError{code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se StoreError) Error() string {
	return se.msg
}
