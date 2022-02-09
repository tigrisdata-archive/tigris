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

package api

import (
	"google.golang.org/protobuf/proto"
)

type RequestType string

const (
	Insert              RequestType = "Insert"
	Replace             RequestType = "Replace"
	Update              RequestType = "Update"
	Delete              RequestType = "Delete"
	Read                RequestType = "Read"
	BeginTransaction    RequestType = "BeginTransaction"
	CommitTransaction   RequestType = "CommitTransaction"
	RollbackTransaction RequestType = "RollbackTransaction"
	CreateCollection    RequestType = "CreateCollection"
	DropCollection      RequestType = "DropCollection"
	AlterCollection     RequestType = "AlterCollection"
	TruncateCollection  RequestType = "TruncateCollection"
)

type Request interface {
	proto.Message
	Validator

	Type() RequestType
}

type Response interface {
	proto.Message
}

func (x *BeginTransactionRequest) Type() RequestType {
	return BeginTransaction
}

func (x *CommitTransactionRequest) Type() RequestType {
	return CommitTransaction
}

func (x *RollbackTransactionRequest) Type() RequestType {
	return RollbackTransaction
}

func (x *InsertRequest) Type() RequestType {
	return Insert
}

func (x *ReplaceRequest) Type() RequestType {
	return Replace
}

func (x *UpdateRequest) Type() RequestType {
	return Update
}

func (x *DeleteRequest) Type() RequestType {
	return Delete
}

func (x *ReadRequest) Type() RequestType {
	return Read
}

func (x *CreateCollectionRequest) Type() RequestType {
	return CreateCollection
}

func (x *DropCollectionRequest) Type() RequestType {
	return DropCollection
}

func (x *AlterCollectionRequest) Type() RequestType {
	return AlterCollection
}

func (x *TruncateCollectionRequest) Type() RequestType {
	return TruncateCollection
}
