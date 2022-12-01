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

package metadata

import "fmt"

type ErrorCode byte

const (
	ErrCodeDatabaseNotFound          ErrorCode = 0x00
	ErrCodeDatabaseExists            ErrorCode = 0x01
	ErrCodeDatabaseBranchExists      ErrorCode = 0x02
	ErrCodeMainBranchCannotBeDeleted ErrorCode = 0x03
	ErrCodeBranchNotFound            ErrorCode = 0x04
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

var (
	MainBranchCannotBeDeletedErr = NewMetadataError(ErrCodeMainBranchCannotBeDeleted, "'main' branch cannot be deleted")
)

func (e Error) Error() string {
	return e.msg
}

func (e Error) Code() ErrorCode {
	return e.code
}

func NewDatabaseNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeDatabaseNotFound, "database doesn't exist '%s'", name)
}

func NewDatabaseExistsErr(name string) error {
	return NewMetadataError(ErrCodeDatabaseExists, "database already exist '%s'", name)
}

func NewDatabaseBranchExistsErr(name string) error {
	return NewMetadataError(ErrCodeDatabaseBranchExists, "branch already exist '%s'", name)
}

func NewBranchNotFoundErr(name string) error {
	return NewMetadataError(ErrCodeBranchNotFound, "database branch doesn't exist '%s'", name)
}
