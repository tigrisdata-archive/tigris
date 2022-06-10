// Copyright 2022 Tigris Data, Inc.
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

package search

import (
	"fmt"
	"net/http"
)

type ErrCode byte

const (
	ErrCodeInvalid           ErrCode = 0x00
	ErrCodeDuplicate         ErrCode = 0x01
	ErrCodeNotFound          ErrCode = 0x02
	ErrCodeIndexingDocuments ErrCode = 0x03
	ErrCodeUnhandled         ErrCode = 0x04
)

var (
	ErrDuplicateEntity = NewSearchError(http.StatusConflict, ErrCodeDuplicate, "entity already exists")
	ErrNotFound        = NewSearchError(http.StatusNotFound, ErrCodeNotFound, "not found")
)

type Error struct {
	httpCode int
	code     ErrCode
	msg      string
	wrapped  error
}

func NewSearchError(httpCode int, code ErrCode, msg string, args ...interface{}) error {
	return Error{httpCode: httpCode, code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se Error) Error() string {
	return se.msg
}

func IsSearchError(err error) bool {
	_, ok := err.(*Error)
	return ok
}
