package kv

import "fmt"

type StoreErrCode byte

const (
	ErrCodeInvalid                StoreErrCode = 0x00
	ErrCodeDuplicateKey           StoreErrCode = 0x01
	ErrCodeConflictingTransaction StoreErrCode = 0x02
)

var (
	// ErrDuplicateKey is returned when an insert call is made for a key that already exist.
	ErrDuplicateKey = NewStoreError(ErrCodeDuplicateKey, "duplicate key value, violates key constraint")
	// ErrConflictingTransaction is returned when there are conflicting transactions.
	ErrConflictingTransaction = NewStoreError(ErrCodeConflictingTransaction, "transaction not committed due to conflict with another transaction")
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
