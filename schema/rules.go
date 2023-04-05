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
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/config"
)

var (
	ErrCollectionNameMismatch = errors.InvalidArgument("mismatch in the collection name")
	ErrIndexNameMismatch      = errors.InvalidArgument("mismatch in the index name")
)

var validators = []Validator{
	&PrimaryIndexSchemaValidator{},
	&FieldSchemaValidator{},
}

var searchIndexValidators = []SearchIndexValidator{
	&FieldSchemaValidator{},
	&SearchIndexSourceValidator{},
	&SearchIdSchemaValidator{},
}

// Validator is a backward compatibility validator i.e. when an update request for a collection is received then a set
// of validators are run to ensure that collection is safe to upgrade from state A to B.
//
// Note: Validators have no role during initial collection creation as there is no backward compatibility check at that
// point.
type Validator interface {
	Validate(existing *DefaultCollection, current *Factory) error
}

// SearchIndexValidator is a backward compatibility validator i.e. when an update request for a search
// index is received then a set of validators are run to ensure that search index is safe to upgrade from
// state A to B.
type SearchIndexValidator interface {
	ValidateIndex(existing *SearchIndex, current *SearchFactory) error
}

type PrimaryIndexSchemaValidator struct{}

func (v *PrimaryIndexSchemaValidator) Validate(existing *DefaultCollection, current *Factory) error {
	return existing.GetPrimaryKey().IsCompatible(current.PrimaryKey)
}

type FieldSchemaValidator struct{}

func (v *FieldSchemaValidator) validateLow(keyPath string, existing []*Field, current []*Field, isMap bool) error {
	currentMap := make(map[string]*Field)
	for _, e := range current {
		currentMap[e.FieldName] = e
	}

	if keyPath != "" {
		keyPath += "."
	}

	for _, f := range existing {
		c, ok := currentMap[f.FieldName]
		if !ok {
			if config.DefaultConfig.Schema.AllowIncompatible {
				continue
			}

			if isMap {
				continue
			}

			return errors.InvalidArgument("removing a field is a backward incompatible change. missing: %s",
				keyPath+f.FieldName)
		}

		if err := f.IsCompatible(keyPath, c); err != nil {
			return err
		}

		if len(f.Fields) == 0 {
			continue // primitive field
		}

		// Validate nested fields
		if err := v.validateLow(keyPath+f.FieldName, f.Fields, c.Fields, c.IsMap()); err != nil {
			return err
		}
	}

	return nil
}

func (v *FieldSchemaValidator) Validate(existing *DefaultCollection, current *Factory) error {
	return v.validateLow("", existing.Fields, current.Fields, false)
}

func (v *FieldSchemaValidator) validateIndexLow(existing []*Field, current []*Field) error {
	currentMap := make(map[string]*Field)
	for _, e := range current {
		currentMap[e.FieldName] = e
	}

	for _, f := range existing {
		f1, ok := currentMap[f.FieldName]
		if !ok {
			// dropping a field is allowed
			continue
		}

		if f.DataType != f1.DataType {
			return errors.InvalidArgument("data type mismatch for field '%s'", f.FieldName)
		}

		// Validate nested fields
		if err := v.validateIndexLow(f.Fields, f1.Fields); err != nil {
			return err
		}
	}

	return nil
}

func (v *FieldSchemaValidator) ValidateIndex(existing *SearchIndex, current *SearchFactory) error {
	return v.validateIndexLow(existing.Fields, current.Fields)
}

type SearchIdSchemaValidator struct{}

func (v *SearchIdSchemaValidator) IdField(current []*Field) *Field {
	for _, f := range current {
		if f.IsSearchId() {
			return f
		}

		if f := v.IdField(f.Fields); f != nil {
			return f
		}
	}

	return nil
}

func (v *SearchIdSchemaValidator) ValidateIndex(existing *SearchIndex, current *SearchFactory) error {
	idField := v.IdField(current.Fields)
	if idField == nil && existing.SearchIDField == nil {
		return nil
	}

	if idField == nil || existing.SearchIDField == nil {
		return errors.InvalidArgument("changing the data type of 'id' field is not allowed")
	}
	if idField.DataType != existing.SearchIDField.DataType {
		return errors.InvalidArgument("changing the data type of 'id' field is not allowed")
	}
	keys := existing.SearchIDField.KeyPath()
	if keys[len(keys)-1] != idField.FieldName {
		return errors.InvalidArgument("renaming 'id' field is not allowed")
	}

	return nil
}

type SearchIndexSourceValidator struct{}

func (v *SearchIndexSourceValidator) ValidateIndex(existing *SearchIndex, current *SearchFactory) error {
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

// ApplyBackwardCompatibilityRules is to validate incoming collection request against the existing present collection. It performs
// following validations,
//   - Primary Key Changed, or order of fields part of the primary key is changed
//   - Collection name change
//   - A validation on field property is also applied like for instance if existing field has some property, but it is
//     removed in the new schema
//   - Any index exist on the collection will also have same checks like type, etc
func ApplyBackwardCompatibilityRules(existing *DefaultCollection, current *Factory) error {
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

// ApplySearchIndexBackwardCompatibilityRules is to validate incoming index schema against the existing index version. It performs
// following validations,
//   - Collection name change.
//   - A validation on field like changing the type is not allowed.
//   - A validation on index source, it is defined during index creation and should not change afterwards.
func ApplySearchIndexBackwardCompatibilityRules(existing *SearchIndex, current *SearchFactory) error {
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

// ValidateFieldBuilder is to validate before building a field. This is done because there are some validation that
// can't be caught in the "ValidateFieldAttributes" like the values for default fields. Anything that needs to be
// validated on builder and can't be done on field has to be done here. Similar to ValidateFieldAttributes, this is
// also only done during incoming requests and ignored during reloading of schemas.
func ValidateFieldBuilder(f FieldBuilder) error {
	fieldType := ToFieldType(f.Type, f.Encoding, f.Format)
	if fieldType == UnknownType {
		if len(f.Encoding) > 0 {
			return errors.InvalidArgument("unsupported encoding '%s'", f.Encoding)
		}
		if len(f.Format) > 0 {
			return errors.InvalidArgument("unsupported format '%s'", f.Format)
		}

		return errors.InvalidArgument("unsupported type detected '%s'", f.Type)
	}

	if f.CreatedAt != nil || f.UpdatedAt != nil || f.Default != nil {
		if _, err := newDefaulter(f.CreatedAt, f.UpdatedAt, f.FieldName, fieldType, f.Default); err != nil {
			return err
		}
	}

	return nil
}

// ValidateFieldAttributes is validated when a schema request is received. Each builder(collection/search) calls this
// method to validate if field is properly formed. This is done outside the fieldBuilder to avoid doing validation
// during reloading of schemas. This method recursively checks each field that whether the attributes are properly set
// or not.
func ValidateFieldAttributes(isSearch bool, field *Field) error {
	if IsReservedField(field.FieldName) {
		return errors.InvalidArgument("following reserved fields are not allowed %q", ReservedFields)
	}

	if isSearch {
		if field.IsPrimaryKey() {
			return errors.InvalidArgument("setting primary key is not supported on search index '%s'", field.Name())
		}

		if field.FieldName == SearchId || field.IsSearchId() {
			if field.DataType != StringType && field.DataType != UUIDType {
				return errors.InvalidArgument("Cannot have field '%s' as 'id'. Only string type is supported as 'id' field", field.FieldName)
			}
		}
	} else {
		if field.IsPrimaryKey() {
			// validate the primary key types
			if !IsValidKeyType(field.DataType) {
				return errors.InvalidArgument("unsupported primary key type detected '%s'", FieldNames[field.DataType])
			}
		}
		if !field.IsPrimaryKey() && field.AutoGenerated != nil && *field.AutoGenerated {
			return errors.InvalidArgument("only primary fields can be set as auto-generated '%s'", field.FieldName)
		}
	}

	if field.DataType == ObjectType {
		if hasIndexingAttributes(field) {
			if field.IsIndexed() {
				return errors.InvalidArgument("Cannot enable index on object '%s' or object fields", field.Name())
			} else {
				if len(field.Fields) > 0 {
					// either index an object or the flattened form i.e. object with fields
					return errors.InvalidArgument("Cannot have search attributes on object '%s', set it on object fields", field.Name())
				} else if field.IsSorted() || field.IsFaceted() {
					return errors.InvalidArgument("Cannot have sort or facet attribute on an object '%s'", field.Name())
				}
			}
		}
		return validateObjectFields(field, false)
	}

	return validateFields(field, false)
}

func validateObjectFields(f *Field, notSupported bool) error {
	for _, nested := range f.Fields {
		if nested.DataType == ObjectType {
			if hasIndexingAttributes(nested) {
				if nested.IsIndexed() {
					return errors.InvalidArgument("Cannot enable index on object '%s' or object fields", nested.Name())
				} else {
					// it needs to be on field level.
					return errors.InvalidArgument("Cannot have search attributes on object '%s', set it on object fields", nested.Name())
				}
			}
			if hasIndexingAttributes(nested) && len(nested.Fields) == 0 {
				// it needs to be on field level.
				return errors.InvalidArgument("Cannot have search attributes on nested object '%s' that has no fields", nested.Name())
			}
			if nested.IsSearchId() {
				return errors.InvalidArgument("Cannot have field '%s' as 'id'. Only string type is supported as 'id' field", nested.FieldName)
			}

			if err := validateObjectFields(nested, notSupported); err != nil {
				return err
			}
		} else {
			if nested.IsIndexed() {
				return errors.InvalidArgument("Cannot enable index on nested field '%s'", nested.Name())
			}
			if nested.DataType == ArrayType && nested.Fields[0].DataType == ObjectType && hasIndexingAttributes(nested) {
				return errors.InvalidArgument("Cannot enable search attributes on nested array of objects '%s'. "+
					"Only top level array of objects can have search index", nested.Name())
			}
			if err := validateFields(nested, notSupported); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateFields(f *Field, notSupported bool) error {
	if f.DataType == ArrayType {
		if f.IsSearchId() {
			return errors.InvalidArgument("Cannot have field '%s' as 'id'. Only string type is supported as 'id' field", f.FieldName)
		}

		if len(f.Fields) == 0 {
			return nil
		}

		if f.Fields[0].DataType != ObjectType {
			return validateFieldAttribute(f, notSupported)
		}

		if f.IsIndexed() || f.Fields[0].IsIndexed() {
			return errors.InvalidArgument("Cannot enable index on an array of objects '%s'", f.FieldName)
		}

		// for arrays, we are validating that there are no attribute set on any nested objects
		return validateObjectFields(f.Fields[0], true)
	}

	return validateFieldAttribute(f, notSupported)
}

func validateFieldAttribute(f *Field, notSupported bool) error {
	// for array elements, items will have field name empty so skip the test for that.
	// make sure they start with [a-z], [A-Z], $, _ and can only contain [a-z], [A-Z], $, _, [0-9]
	if len(f.FieldName) > 0 && !ValidFieldNamePattern.MatchString(f.FieldName) {
		return errors.InvalidArgument(MsgFieldNameInvalidPattern, f.FieldName)
	}

	if notSupported && hasIndexingAttributes(f) {
		return errors.InvalidArgument("Cannot enable index or search on an array of objects '%s'", f.FieldName)
	}

	subType := UnknownType
	if f.DataType == ArrayType {
		if hasIndexingAttributes(f.Fields[0]) {
			return errors.InvalidArgument("Attributes for primitive arrays needs to be set on array level '%s'", f.FieldName)
		}

		subType = f.Fields[0].DataType
	}

	if f.IsSearchId() && f.DataType != StringType && f.DataType != UUIDType {
		return errors.InvalidArgument("Cannot have field '%s' as 'id'. Only string type is supported as 'id' field", f.FieldName)
	}
	if f.DataType == VectorType {
		if f.Dimensions == nil {
			return errors.InvalidArgument("Field '%s' type 'vector' is missing dimensions ", f.FieldName)
		}
		if f.IsIndexed() || f.IsFaceted() || f.IsSorted() {
			return errors.InvalidArgument("only search index attribute is supported on vector field '%s'", f.FieldName)
		}
	}
	if f.IsIndexed() && !f.IsIndexable() {
		return errors.InvalidArgument("Cannot enable index on field '%s' of type '%s'. Only top level non-byte fields can be indexed.", f.FieldName, FieldNames[f.DataType])
	}
	if f.IsSearchIndexed() && !SupportedSearchIndexableType(f.DataType, subType) {
		return errors.InvalidArgument("Cannot enable search index on field '%s' of type '%s'", f.FieldName, FieldNames[f.DataType])
	}
	if !f.IsSearchIndexed() && (f.IsFaceted() || f.IsSorted()) {
		return errors.InvalidArgument("Enable search index first to use faceting or sorting on field '%s' of type '%s'", f.FieldName, FieldNames[f.DataType])
	}
	if f.IsFaceted() && !SupportedFacetableType(f.DataType, subType) {
		return errors.InvalidArgument("Cannot enable faceting on field '%s'", f.FieldName)
	}
	if f.IsSorted() && !IsPrimitiveType(f.DataType) {
		return errors.InvalidArgument("Cannot enable sorting on field '%s' of type '%s'", f.FieldName, FieldNames[f.DataType])
	}

	return nil
}

func hasIndexingAttributes(f *Field) bool {
	return f.IsIndexed() || f.IsSearchIndexed() || f.IsFaceted() || f.IsSorted()
}

// ValidateSupportedProperties to validate what all JSON tagging is allowed on a field. We can extend this to also
// validate based on whether the schema is for search or for collection.
func ValidateSupportedProperties(v []byte) error {
	var fieldProperties map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(v, &fieldProperties); err != nil {
		return err
	}

	for key := range fieldProperties {
		if !SupportedFieldProperties.Contains(key) {
			return errors.InvalidArgument("unsupported property found '%s'", key)
		}
	}

	return nil
}
