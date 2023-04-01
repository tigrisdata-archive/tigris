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

package api

import (
	"regexp"
)

var validNamePattern = regexp.MustCompile("^[a-zA-Z_]+[a-zA-Z0-9_-]+$")

type Validator interface {
	Validate() error
}

func (x *BeginTransactionRequest) Validate() error {
	return isValidDatabase(x.Project)
}

func (x *CommitTransactionRequest) Validate() error {
	return isValidDatabase(x.Project)
}

func (x *RollbackTransactionRequest) Validate() error {
	return isValidDatabase(x.Project)
}

func (x *InsertRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty documents received")
	}
	return nil
}

func (x *ReplaceRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty documents received")
	}
	return nil
}

func (x *UpdateRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.GetFields()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty fields received")
	}

	if len(x.GetFilter()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "filter is a required field")
	}
	if x.Options != nil && x.Options.Collation != nil {
		if err := x.Options.Collation.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (x *DeleteRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.GetFilter()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "filter is a required field")
	}
	if x.Options != nil && x.Options.Collation != nil {
		if err := x.Options.Collation.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (x *ReadRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.GetFilter()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "filter is a required field")
	}
	if x.Options != nil && x.Options.Collation != nil {
		if err := x.Options.Collation.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (x *SearchRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if len(x.IncludeFields) > 0 && len(x.ExcludeFields) > 0 {
		return Errorf(Code_INVALID_ARGUMENT, "Cannot use both `include_fields` and `exclude_fields` together")
	}

	if err := isValidPaginationParam("page", int(x.Page)); err != nil {
		return err
	}

	if err := isValidPaginationParam("page_size", int(x.PageSize)); err != nil {
		return err
	}
	if x.Collation != nil {
		if err := x.Collation.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (x *CreateOrUpdateCollectionRequest) Validate() error {
	if err := isValidCollectionAndDatabase(x.Collection, x.Project); err != nil {
		return err
	}

	if x.Schema == nil {
		return Errorf(Code_INVALID_ARGUMENT, "schema is a required during collection creation")
	}

	return nil
}

func (x *DropCollectionRequest) Validate() error {
	return isValidCollectionAndDatabase(x.Collection, x.Project)
}

func (*ListCollectionsRequest) Validate() error {
	return nil
}

func (*DescribeCollectionRequest) Validate() error {
	return nil
}

func (*DescribeDatabaseRequest) Validate() error {
	return nil
}

func (x *CreateProjectRequest) Validate() error {
	return isValidDatabase(x.Project)
}

func (x *DeleteProjectRequest) Validate() error {
	return isValidDatabase(x.Project)
}

func (*ListProjectsRequest) Validate() error {
	return nil
}

func (x *CreateOrUpdateIndexRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Name); err != nil {
		return err
	}

	if len(x.Schema) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "schema is a required during index creation")
	}

	return nil
}

func (x *DeleteIndexRequest) Validate() error {
	return isValidProjectAndSearchIndex(x.Project, x.Name)
}

func (x *GetIndexRequest) Validate() error {
	return isValidProjectAndSearchIndex(x.Project, x.Name)
}

func (x *CreateDocumentRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Index); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty documents received")
	}
	return nil
}

func (x *CreateOrReplaceDocumentRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Index); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty documents received")
	}
	return nil
}

func (x *UpdateDocumentRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Index); err != nil {
		return err
	}

	if len(x.GetDocuments()) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "received no documents to update")
	}
	return nil
}

func (x *DeleteDocumentRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Index); err != nil {
		return err
	}

	if len(x.Ids) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "no 'ids' to delete")
	}
	return nil
}

func (x *GetDocumentRequest) Validate() error {
	if err := isValidProjectAndSearchIndex(x.Project, x.Index); err != nil {
		return err
	}

	if len(x.Ids) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "'ids' is a required field")
	}
	return nil
}

func isValidCollection(name string) error {
	if len(name) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "invalid collection name")
	}
	if !validNamePattern.MatchString(name) {
		return Errorf(Code_INVALID_ARGUMENT, "invalid collection name")
	}
	return nil
}

func isValidDatabase(name string) error {
	if len(name) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "invalid database name")
	}
	if !validNamePattern.MatchString(name) {
		return Errorf(Code_INVALID_ARGUMENT, "invalid database name")
	}
	return nil
}

func isValidSearchIndexName(name string) error {
	if len(name) == 0 {
		return Errorf(Code_INVALID_ARGUMENT, "empty search index name is not allowed")
	}
	if !validNamePattern.MatchString(name) {
		return Errorf(Code_INVALID_ARGUMENT, "invalid search index name '%s'", name)
	}
	return nil
}

func isValidCollectionAndDatabase(c string, db string) error {
	if err := isValidCollection(c); err != nil {
		return err
	}

	return isValidDatabase(db)
}

func isValidProjectAndSearchIndex(project string, index string) error {
	if err := isValidDatabase(project); err != nil {
		return err
	}

	return isValidSearchIndexName(index)
}

func isValidPaginationParam(param string, value int) error {
	if value < 0 {
		return Errorf(Code_INVALID_ARGUMENT, "invalid value for `%s`", param)
	}
	return nil
}
