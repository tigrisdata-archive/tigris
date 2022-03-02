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
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	Insert              protoreflect.FullName = "InsertRequest"
	Replace             protoreflect.FullName = "ReplaceRequest"
	Update              protoreflect.FullName = "UpdateRequest"
	Delete              protoreflect.FullName = "DeleteRequest"
	Read                protoreflect.FullName = "ReadRequest"
	BeginTransaction    protoreflect.FullName = "BeginTransactionRequest"
	CommitTransaction   protoreflect.FullName = "CommitTransactionRequest"
	RollbackTransaction protoreflect.FullName = "RollbackTransactionRequest"
	CreateCollection    protoreflect.FullName = "CreateCollectionRequest"
	DropCollection      protoreflect.FullName = "DropCollectionRequest"
	AlterCollection     protoreflect.FullName = "AlterCollectionRequest"
	TruncateCollection  protoreflect.FullName = "TruncateCollectionRequest"
	ListCollections     protoreflect.FullName = "ListCollectionsRequest"
	ListDatabases       protoreflect.FullName = "ListDatabasesRequest"
	CreateDatabase      protoreflect.FullName = "CreateDatabaseRequest"
	DropDatabase        protoreflect.FullName = "DropDatabaseRequest"
	CreateProject       protoreflect.FullName = "CreateProjectRequest"
	ListProjects        protoreflect.FullName = "ListProjectsRequest"
	DeleteProject       protoreflect.FullName = "DeleteProjectRequest"
)

type Request interface {
	proto.Message
	Validator
}

type Response interface {
	proto.Message
}

var RequestType = proto.MessageName
