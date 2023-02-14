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

package metadata

import "fmt"

type ErrorCode byte

const (
	ErrCodeDatabaseNotFound     ErrorCode = 0x00
	ErrCodeDatabaseExists       ErrorCode = 0x01
	ErrCodeDatabaseBranchExists ErrorCode = 0x02
	ErrCodeBranchNotFound       ErrorCode = 0x03
	ErrCodeCannotDeleteBranch   ErrorCode = 0x04
	ErrCodeProjectNotFound      ErrorCode = 0x05
	ErrCodeSearchIndexExists    ErrorCode = 0x06
	ErrCodeSearchIndexNotFound  ErrorCode = 0x07
	ErrCodeCacheExists          ErrorCode = 0x08
	ErrCodeCacheNotFound        ErrorCode = 0x09

	ErrDBMismatch     ErrorCode = 0x0A
	ErrBranchMismatch ErrorCode = 0x0B
)

type Error struct {
	code ErrorCode
	msg  string
}

func NewMetadataError(code ErrorCode, msg string, args ...interface{}) error {
	return Error{
		code: code,
		msg:  fmt.Sprintf(msg, args...),
	}
}

func (e Error) Error() string {
	return e.msg
}

func (e Error) Code() ErrorCode {
	return e.code
}

func NewDatabaseBranchExistsErr(name string) error {
	return NewMetadataError(ErrCodeDatabaseBranchExists, "branch already exist '%s'", name)
}

func NewBranchNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeBranchNotFound, "database branch doesn't exist '%s'", name)
}

func NewProjectNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeProjectNotFound, "project doesn't exist '%s'", name)
}

func NewSearchIndexExistsErr(name string) error {
	return NewMetadataError(ErrCodeSearchIndexExists, "search index already exist '%s'", name)
}

func NewSearchIndexNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeSearchIndexNotFound, "search index not found '%s'", name)
}

func NewCacheExistsErr(name string) error {
	return NewMetadataError(ErrCodeCacheExists, "cache already exist '%s'", name)
}

func NewCacheNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeCacheNotFound, "cache not found '%s'", name)
}

func NewBranchMismatchErr(old string, newName string) error {
	return NewMetadataError(ErrBranchMismatch, "branch mismatch was: '%s', got: '%s'", old, newName)
}

func NewDatabaseMismatchErr(old string, newName string) error {
	return NewMetadataError(ErrDBMismatch, "database mismatch was: '%s', got: '%s'", old, newName)
}
