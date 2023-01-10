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

package cache

import (
	"fmt"
)

type ErrCode byte

const (
	errStrNoSuchKey              = "ERR no such key"
	errStrConsGroupAlreadyExists = "Consumer Group name already exists"
	CacheAlreadyExist            = "Cache already exist"
	CacheNotFound                = "Cache not found"
)

const (
	ErrCodeStreamExists     ErrCode = 0x01
	ErrCodeStreamNotFound   ErrCode = 0x02
	ErrCodeKeyNotFound      ErrCode = 0x03
	ErrCodeKeyAlreadyExists ErrCode = 0x04
	ErrCodeEmptyKey         ErrCode = 0x05
)

var (
	// ErrStreamAlreadyExists is returned when a stream already exists.
	ErrStreamAlreadyExists = NewCacheError(ErrCodeStreamExists, "stream already exists")
	// ErrStreamNotFound is returned when a stream does not exist.
	ErrStreamNotFound   = NewCacheError(ErrCodeStreamNotFound, "stream not found")
	ErrKeyNotFound      = NewCacheError(ErrCodeKeyNotFound, "key not found")
	ErrKeyAlreadyExists = NewCacheError(ErrCodeKeyAlreadyExists, "key already exists")
	ErrEmptyKey         = NewCacheError(ErrCodeEmptyKey, "key is empty")
)

type Error struct {
	code ErrCode
	msg  string
}

func NewCacheError(code ErrCode, msg string, args ...interface{}) error {
	return Error{code: code, msg: fmt.Sprintf(msg, args...)}
}

func (se Error) Error() string {
	return se.msg
}

func IsStreamAlreadyExists(err error) bool {
	return err == ErrStreamAlreadyExists
}
