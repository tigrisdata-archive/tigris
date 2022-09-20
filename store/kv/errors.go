// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	ErrCodeInvalid                StoreErrCode = 0x00
	ErrCodeDuplicateKey           StoreErrCode = 0x01
	ErrCodeConflictingTransaction StoreErrCode = 0x02
	ErrCodeTransactionMaxDuration StoreErrCode = 0x03
)

var (
	// ErrDuplicateKey is returned when an insert call is made for a key that already exist.
	ErrDuplicateKey = NewStoreError(ErrCodeDuplicateKey, "duplicate key value, violates key constraint")
	// ErrConflictingTransaction is returned when there are conflicting transactions.
	ErrConflictingTransaction = NewStoreError(ErrCodeConflictingTransaction, "transaction not committed due to conflict with another transaction")
	// ErrTransactionMaxDurationReached is returned when transaction running beyond 5seconds.
	ErrTransactionMaxDurationReached = NewStoreError(ErrCodeTransactionMaxDuration, "transaction is old to perform reads or be committed")
)

type StoreError struct {
	code StoreErrCode
	msg  string
}

func NewStoreError(code StoreErrCode, msg string, args ...interface{}) error {
	return StoreError{code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se StoreError) Error() string {
	return se.msg
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
