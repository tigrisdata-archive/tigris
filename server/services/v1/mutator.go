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

package v1

import (
	"strconv"
	"strings"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
)

type mutator interface {
	isMutated() bool
	stringToInt64(doc map[string]any) error
	setDefaultsInIncomingPayload(doc map[string]any) error
	setDefaultsInExistingPayload(doc map[string]any) error
}

type baseMutator struct {
	mutated    bool
	collection *schema.DefaultCollection
}

type insertPayloadMutator struct {
	*baseMutator

	createdAt string
}

func newInsertPayloadMutator(collection *schema.DefaultCollection, createdAt string) mutator {
	return &insertPayloadMutator{
		baseMutator: &baseMutator{
			mutated:    false,
			collection: collection,
		},

		createdAt: createdAt,
	}
}

func (mutator *insertPayloadMutator) setDefaultsInIncomingPayload(doc map[string]any) error {
	return mutator.setDefaultsInternal(mutator.collection.TaggedDefaultsForInsert(), doc, mutator.setDefaults)
}

func (mutator *insertPayloadMutator) setDefaultsInExistingPayload(_ map[string]any) error {
	return nil
}

func (mutator *insertPayloadMutator) setDefaults(doc map[string]any, field *schema.Field) {
	if _, ok := doc[field.FieldName]; ok {
		return
	}

	if field.Defaulter.TaggedWithCreatedAt() {
		mutator.mutated = true
		doc[field.FieldName] = mutator.createdAt
	}
	if defaultValue := field.Defaulter.GetValue(); defaultValue != nil {
		mutator.mutated = true
		doc[field.FieldName] = defaultValue
	}
}

type updatePayloadMutator struct {
	*baseMutator

	updatedAt string
}

func newUpdatePayloadMutator(collection *schema.DefaultCollection, updatedAt string) mutator {
	return &updatePayloadMutator{
		baseMutator: &baseMutator{
			mutated:    false,
			collection: collection,
		},

		updatedAt: updatedAt,
	}
}

func (mutator *updatePayloadMutator) setDefaultsInIncomingPayload(_ map[string]any) error {
	// no need to update anything in the incoming payload for update requests
	return nil
}

func (mutator *updatePayloadMutator) setDefaultsInExistingPayload(doc map[string]any) error {
	// we need to update the updatedAt for the payload that we have in the database
	return mutator.setDefaultsInternal(mutator.collection.TaggedDefaultsForUpdate(), doc, mutator.setDefaults)
}

// setDefaults ensures that only updatedAt tag is updated during update request.
func (mutator *updatePayloadMutator) setDefaults(doc map[string]any, field *schema.Field) {
	if _, ok := doc[field.FieldName]; ok {
		return
	}

	if field.Defaulter != nil && field.Defaulter.TaggedWithUpdatedAt() {
		mutator.mutated = true
		doc[field.FieldName] = mutator.updatedAt
	}
}

func (p *baseMutator) isMutated() bool {
	return p.mutated
}

func (p *baseMutator) setDefaultsInternal(taggedFields map[string]struct{}, doc map[string]any, setCB func(doc map[string]any, field *schema.Field)) error {
	for key := range taggedFields {
		keys := strings.Split(key, ".")
		field := p.collection.GetField(keys[0])
		if field == nil {
			continue
		}

		value, ok := doc[keys[0]]
		if ok && len(keys) == 1 {
			// top level and field is set in the payload
			continue
		}

		if !ok {
			if len(keys) == 1 {
				// top level default field
				setCB(doc, field)
				continue
			} else if field.DataType == schema.ObjectType {
				value = map[string]any{}
				doc[field.FieldName] = value
			}
		}

		if err := p.traverseDefaults(doc, keys[1:], field, value, setCB); err != nil {
			return err
		}
	}

	return nil
}

func (p *baseMutator) traverseDefaults(parentMap map[string]any, keys []string, field *schema.Field, value any, setCB func(doc map[string]any, field *schema.Field)) error {
	if field.Defaulter != nil {
		if _, ok := parentMap[field.FieldName]; !ok {
			setCB(parentMap, field)
		}
		return nil
	}

	switch field.DataType {
	case schema.ObjectType:
		converted, ok := value.(map[string]any)
		if !ok {
			// should not happen
			return nil
		}

		value, ok := converted[keys[0]]
		if !ok && field.GetNestedField(keys[0]).DataType == schema.ObjectType {
			// add the object if it is missing in the payload
			value = map[string]any{}
			converted[keys[0]] = value
		}
		return p.traverseDefaults(converted, keys[1:], field.GetNestedField(keys[0]), value, setCB)
	case schema.ArrayType:
		if value == nil {
			// array of objects will only be filled when there are already elements in the array
			return nil
		}

		// only array of object should reach here, primitive arrays are handled by the caller.
		converted, ok := value.([]any)
		if !ok {
			// should not happen
			return nil
		}

		for _, va := range converted {
			if err := p.traverseDefaults(va.(map[string]any), keys, field.Fields[0], va, setCB); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *baseMutator) stringToInt64(doc map[string]any) error {
	for key := range p.collection.GetInt64FieldsPath() {
		keys := strings.Split(key, ".")
		value, ok := doc[keys[0]]
		if !ok {
			continue
		}

		field := p.collection.GetField(keys[0])
		if field == nil {
			continue
		}

		if err := p.traverse(doc, value, keys[1:], field); err != nil {
			return err
		}
	}

	return nil
}

func (p *baseMutator) traverse(parentMap map[string]any, value any, keys []string, parentField *schema.Field) error {
	var err error
	if parentField.Type() == schema.Int64Type {
		if conv, ok := value.(string); ok {
			if parentMap[parentField.FieldName], err = strconv.ParseInt(conv, 10, 64); err != nil {
				return errors.InvalidArgument("json schema validation failed for field '%s' reason 'expected integer, but got string'", parentField.FieldName)
			}

			p.mutated = true
			return nil
		}
	}

	switch converted := value.(type) {
	case map[string]any:
		value, ok := converted[keys[0]]
		if !ok {
			return nil
		}

		return p.traverse(converted, value, keys[1:], parentField.GetNestedField(keys[0]))
	case []any:
		// array should have a single nested field either as object or primitive type
		field := parentField.Fields[0]
		if field.DataType == schema.ObjectType {
			// is object or simple type
			for _, va := range converted {
				if err := p.traverse(va.(map[string]any), va, keys, field); err != nil {
					return err
				}
			}
		} else if field.DataType == schema.Int64Type {
			for idx := range converted {
				if conv, ok := converted[idx].(string); ok {
					if converted[idx], err = strconv.ParseInt(conv, 10, 64); err != nil {
						return errors.InvalidArgument("json schema validation failed for field '%s' reason 'expected integer, but got string'", field.FieldName)
					}
					p.mutated = true
				}
			}
		}
	}

	return nil
}
