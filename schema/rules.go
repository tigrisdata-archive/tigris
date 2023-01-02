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
)

var (
	ErrMissingField           = errors.InvalidArgument("removing a field is a backward incompatible change")
	ErrCollectionNameMismatch = errors.InvalidArgument("mismatch in the collection name")
	// ErrModifiedPrimaryKey     = errors.InvalidArgument("changing primary key is a backward incompatible change").
)

var validators = []Validator{
	&IndexSchemaValidator{},
	&FieldSchemaValidator{},
}

type Validator interface {
	Validate(existing *DefaultCollection, current *Factory) error
}

type IndexSchemaValidator struct{}

func (v *IndexSchemaValidator) Validate(existing *DefaultCollection, current *Factory) error {
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
			return ErrMissingField
		}

		if err := f.IsCompatible(c); err != nil {
			return err
		}
	}

	return nil
}

// ApplySchemaRules is to validate incoming collection request against the existing present collection. It performs
// following validations,
//   - Primary Key Changed, or order of fields part of the primary key is changed
//   - Collection name change
//   - Type of existing field is changed
//   - A validation on field property is also applied like for instance if existing field has some property, but it is
//     removed in the new schema
//   - Removing a field
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
