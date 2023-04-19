// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type StoreErrCode byte

const (
	ErrCodeInvalid                 StoreErrCode = 0x00
	ErrCodeDuplicateKey            StoreErrCode = 0x01
	ErrCodeConflictingTransaction  StoreErrCode = 0x02
	ErrCodeTransactionMaxDuration  StoreErrCode = 0x03
	ErrCodeTransactionTimedOut     StoreErrCode = 0x04
	ErrCodeTransactionNotCommitted StoreErrCode = 0x05
	ErrCodeValueSizeExceeded       StoreErrCode = 0x06
	ErrCodeTransactionSizeExceeded StoreErrCode = 0x07
	ErrCodeNotFound                StoreErrCode = 0x08
)

var (
	// ErrDuplicateKey is returned when an insert call is made for a key that already exist.
	ErrDuplicateKey = NewStoreError(0, ErrCodeDuplicateKey, "duplicate key value, violates key constraint")
	// ErrConflictingTransaction is returned when there are conflicting transactions.
	ErrConflictingTransaction = NewStoreError(1020, ErrCodeConflictingTransaction, "transaction not committed due to conflict with another transaction")
	// ErrTransactionMaxDurationReached is returned when transaction running beyond 5seconds.
	ErrTransactionMaxDurationReached = NewStoreError(1007, ErrCodeTransactionMaxDuration, "transaction is old to perform reads or be committed")
	// ErrTransactionTimedOut is returned when fdb abort the transaction because of 5seconds limit.
	ErrTransactionTimedOut     = NewStoreError(1031, ErrCodeTransactionTimedOut, "operation aborted because the transaction timed out")
	ErrTransactionNotCommitted = NewStoreError(1021, ErrCodeTransactionNotCommitted, "transaction may or may not have committed")
	ErrValueSizeExceeded       = NewStoreError(2103, ErrCodeValueSizeExceeded, "document exceeds limit")
	ErrTransactionSizeExceeded = NewStoreError(2101, ErrCodeTransactionSizeExceeded, "transaction exceeds limit")
	ErrNotFound                = NewStoreError(0, ErrCodeNotFound, "not found")
)

type StoreError struct {
	code    StoreErrCode
	fdbCode int
	msg     string
}

func NewStoreError(fdbCode int, code StoreErrCode, msg string, args ...interface{}) error {
	return StoreError{fdbCode: fdbCode, code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se StoreError) Code() StoreErrCode {
	return se.code
}

func (se StoreError) Msg() string {
	return se.msg
}

func (se StoreError) Error() string {
	return fmt.Sprintf("fdb_code: %d, msg: %s", se.fdbCode, se.msg)
}

func IsTimedOut(err error) bool {
	var ep fdb.Error
	if !errors.As(err, &ep) {
		return false
	}

	// from https://apple.github.io/foundationdb/api-error-codes.html
	// 1004 timed_out
	// 1031 transaction_timed_out
	return ep.Code == 1004 || ep.Code == 1031
}

func convertFDBToStoreErr(fdbErr error) error {
	var ep fdb.Error
	if errors.As(fdbErr, &ep) {
		switch ep.Code {
		case 1020:
			return ErrConflictingTransaction
		case 1007:
			return ErrTransactionMaxDurationReached
		case 1031:
			return ErrTransactionTimedOut
		case 1021:
			return ErrTransactionNotCommitted
		case 2103:
			return ErrValueSizeExceeded
		case 2101:
			return ErrTransactionSizeExceeded
		}
	}

	return fdbErr
}
