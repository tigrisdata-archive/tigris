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
	"google.golang.org/grpc/codes"
)

type Validator interface {
	Validate() error
}

func (x *BeginTransactionRequest) Validate() error {
	if err := isValidDatabase(x.Db); err != nil {
		return err
	}

	return nil
}

func (x *CommitTransactionRequest) Validate() error {
	if err := isValidDatabase(x.Db); err != nil {
		return err
	}

	return nil
}

func (x *RollbackTransactionRequest) Validate() error {
	if err := isValidDatabase(x.Db); err != nil {
		return err
	}

	return nil
}

func (x *InsertRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(codes.InvalidArgument, "empty documents received")
	}
	return nil
}

func (x *ReplaceRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(codes.InvalidArgument, "empty documents received")
	}
	return nil
}

func (x *UpdateRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if len(x.GetFields()) == 0 {
		return Errorf(codes.InvalidArgument, "empty fields received")
	}

	if len(x.GetFilter()) == 0 {
		return Errorf(codes.InvalidArgument, "filter is a required field")
	}
	return nil
}

func (x *DeleteRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if len(x.GetFilter()) == 0 {
		return Errorf(codes.InvalidArgument, "filter is a required field")
	}
	return nil
}

func (x *ReadRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	return nil
}

func (x *CreateOrUpdateCollectionRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if x.Schema == nil {
		return Errorf(codes.InvalidArgument, "schema is a required during collection creation")
	}

	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func (x *DropCollectionRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Db); err != nil {
		return err
	}

	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func (x *ListCollectionsRequest) Validate() error {
	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func (x *CreateDatabaseRequest) Validate() error {
	if err := isValidDatabase(x.Db); err != nil {
		return err
	}

	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func (x *DropDatabaseRequest) Validate() error {
	if err := isValidDatabase(x.Db); err != nil {
		return err
	}

	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func (x *ListDatabasesRequest) Validate() error {
	if !IsTxSupported(x) {
		if transaction := GetTransaction(x); transaction != nil {
			return Errorf(codes.InvalidArgument, "interactive tx not supported but transaction token found")
		}
	}

	return nil
}

func isValidCollection(name string) error {
	if len(name) == 0 {
		return Error(codes.InvalidArgument, "invalid collection name")
	}

	return nil
}

func isValidDatabase(name string) error {
	if len(name) == 0 {
		return Error(codes.InvalidArgument, "invalid database name")
	}

	return nil
}

func isValidCollectionAndDatabase(c string, db string) error {
	if err := isValidCollection(c); err != nil {
		return err
	}

	if err := isValidDatabase(db); err != nil {
		return err
	}

	return nil
}
