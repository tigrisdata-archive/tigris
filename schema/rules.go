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

package schema

import (
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
)

var (
	ErrMissingField           = errors.InvalidArgument("removing a field is a backward incompatible change")
	ErrCollectionNameMismatch = errors.InvalidArgument("mismatch in the collection name")
	ErrIndexNameMismatch      = errors.InvalidArgument("mismatch in the index name")
)

var validators = []Validator{
	&PrimaryIndexSchemaValidator{},
	&FieldSchemaValidator{},
}

var searchIndexValidators = []SearchIndexValidator{
	&FieldSchemaValidator{},
	&IndexSourceValidator{},
}

type Validator interface {
	Validate(existing *DefaultCollection, current *Factory) error
}

type SearchIndexValidator interface {
	ValidateIndex(existing *SearchIndex, current *SearchFactory) error
}

type PrimaryIndexSchemaValidator struct{}

func (v *PrimaryIndexSchemaValidator) Validate(existing *DefaultCollection, current *Factory) error {
	return existing.Indexes.PrimaryKey.IsCompatible(current.Indexes.PrimaryKey)
}

type FieldSchemaValidator struct{}

func (v *FieldSchemaValidator) Validate(existing *DefaultCollection, current *Factory) error {
	existingFields := make(map[string]*Field)
	for _, e := range existing.Fields {
		existingFields[e.FieldName] = e
	}

	currentFields := make(map[string]*Field)
	for _, e := range current.Fields {
		currentFields[e.FieldName] = e
	}

	for name, f := range existingFields {
		c, ok := currentFields[name]
		if !ok {
			if config.DefaultConfig.Schema.AllowIncompatible {
				continue
			}

			return ErrMissingField
		}

		if err := f.IsCompatible(c); err != nil {
			return err
		}
	}

	return nil
}

func (v *FieldSchemaValidator) ValidateIndex(existing *SearchIndex, current *SearchFactory) error {
	existingFields := make(map[string]*Field)
	for _, e := range existing.Fields {
		existingFields[e.FieldName] = e
	}

	currentFields := make(map[string]*Field)
	for _, e := range current.Fields {
		currentFields[e.FieldName] = e
	}

	for name, f := range existingFields {
		f1, ok := currentFields[name]
		if !ok {
			// dropping a field is allowed
			continue
		}

		if f.DataType != f1.DataType {
			return errors.InvalidArgument("data type mismatch for field '%s'", f.FieldName)
		}
	}

	return nil
}

type IndexSourceValidator struct{}

func (v *IndexSourceValidator) ValidateIndex(existing *SearchIndex, current *SearchFactory) error {
	if len(existing.Source.Type) > 0 && existing.Source.Type != current.Source.Type {
		if existing.Source.Type == "user" && current.Source.Type == SearchSourceExternal {
			return nil
		}

		return errors.InvalidArgument(
			"changing index source type is not allowed from: '%s', to: '%s'",
			existing.Source.Type,
			current.Source.Type,
		)
	}

	if len(existing.Source.CollectionName) > 0 && existing.Source.CollectionName != current.Source.CollectionName {
		return errors.InvalidArgument(
			"changing index source collection is not allowed from: '%s', to: '%s'",
			existing.Source.CollectionName,
			current.Source.CollectionName,
		)
	}

	if len(existing.Source.DatabaseBranch) > 0 {
		if len(current.Source.DatabaseBranch) > 0 && existing.Source.DatabaseBranch != current.Source.DatabaseBranch {
			return errors.InvalidArgument(
				"changing index source database branch is not allowed from: '%s', to: '%s'",
				existing.Source.DatabaseBranch,
				current.Source.DatabaseBranch,
			)
		}
	}

	return nil
}

// ApplySchemaRules is to validate incoming collection request against the existing present collection. It performs
// following validations,
//   - Primary Key Changed, or order of fields part of the primary key is changed
//   - Collection name change
//   - A validation on field property is also applied like for instance if existing field has some property, but it is
//     removed in the new schema
//   - Any index exist on the collection will also have same checks like type, etc
func ApplySchemaRules(existing *DefaultCollection, current *Factory) error {
	if existing.Name != current.Name {
		return ErrCollectionNameMismatch
	}

	for _, v := range validators {
		if err := v.Validate(existing, current); err != nil {
			return err
		}
	}

	return nil
}

// ApplySearchIndexSchemaRules is to validate incoming index schema against the existing index version. It performs
// following validations,
//   - Collection name change.
//   - A validation on field like changing the type is not allowed.
//   - A validation on index source, it is defined during index creation and should not change afterwards.
func ApplySearchIndexSchemaRules(existing *SearchIndex, current *SearchFactory) error {
	if existing.Name != current.Name {
		return ErrIndexNameMismatch
	}

	for _, v := range searchIndexValidators {
		if err := v.ValidateIndex(existing, current); err != nil {
			return err
		}
	}

	return nil
}
