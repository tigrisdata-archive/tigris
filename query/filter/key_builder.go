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

package filter

import (
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
)

// KeyBuilder is responsible for building internal Keys. A composer is caller by the builder to build the internal keys
// based on the Composer logic.
type KeyBuilder struct {
	composer KeyComposer
}

// NewKeyBuilder returns a KeyBuilder
func NewKeyBuilder(composer KeyComposer) *KeyBuilder {
	return &KeyBuilder{
		composer: composer,
	}
}

// Build is responsible for building the internal keys from the user filter and using the keys defined in the schema
// and passed by the caller in this method. The build is doing a level by level traversal to build the internal Keys.
// On each level multiple keys can be formed because the user can specify ranges. The builder is not deciding the logic
// of key generation, the builder is simply traversing on the filters and calling compose where the logic resides.
func (k *KeyBuilder) Build(filters []Filter, userDefinedKeys []*schema.Field) ([]keys.Key, error) {
	var queue []Filter
	var singleLevel []*Selector
	var allKeys []keys.Key
	for _, f := range filters {
		if ss, ok := f.(*Selector); ok {
			singleLevel = append(singleLevel, ss)
		} else {
			queue = append(queue, f)
		}
	}
	if len(singleLevel) > 0 {
		// if we have something on top level
		iKeys, err := k.composer.Compose(singleLevel, userDefinedKeys, AndOP)
		if err != nil {
			return nil, err
		}
		allKeys = append(allKeys, iKeys...)
	}

	for len(queue) > 0 {
		element := queue[0]
		if e, ok := element.(LogicalFilter); ok {
			var singleLevel []*Selector
			for _, ee := range e.GetFilters() {
				if ss, ok := ee.(*Selector); ok {
					singleLevel = append(singleLevel, ss)
				} else {
					queue = append(queue, ee)
				}
			}

			if len(singleLevel) > 0 {
				// try building keys with there is selector available
				iKeys, err := k.composer.Compose(singleLevel, userDefinedKeys, e.Type())
				if err != nil {
					return nil, err
				}
				allKeys = append(allKeys, iKeys...)
			}
		}
		queue = queue[1:]
	}

	return allKeys, nil
}

// KeyComposer needs to be implemented to have a custom Compose method with different constraints.
type KeyComposer interface {
	Compose(level []*Selector, userDefinedKeys []*schema.Field, parent LogicalOP) ([]keys.Key, error)
}

// StrictEqKeyComposer is to generate internal keys only if the condition is equality on the fields that are part of
// the schema and all these fields are present in the filters. The following rules are applied for StrictEqKeyComposer
//  - The userDefinedKeys(indexes defined in the schema) passed in parameter should be present in the filter
//  - For AND filters it is possible to build internal keys for composite indexes, for OR it is not possible.
// So for OR filter an error is returned if it is used for indexes that are composite.
type StrictEqKeyComposer struct {
	// keyEncodingFunc returns encoded key from index parts
	keyEncodingFunc func(indexParts ...interface{}) (keys.Key, error)
}

func NewStrictEqKeyComposer(keyEncodingFunc func(indexParts ...interface{}) (keys.Key, error)) *StrictEqKeyComposer {
	return &StrictEqKeyComposer{
		keyEncodingFunc: keyEncodingFunc,
	}
}

// Compose is implementing the logic of composing keys
func (s *StrictEqKeyComposer) Compose(selectors []*Selector, userDefinedKeys []*schema.Field, parent LogicalOP) ([]keys.Key, error) {
	var compositeKeys = make([][]*Selector, 1) // allocate just for the first keyParts
	for _, k := range userDefinedKeys {
		var repeatedFields []*Selector
		for _, sel := range selectors {
			if k.FieldName == sel.Field.Name() {
				repeatedFields = append(repeatedFields, sel)
			}
			if sel.Matcher.Type() != EQ {
				return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "filters only supporting $eq comparison, found '%s'", sel.Matcher.Type())
			}
		}

		if len(repeatedFields) == 0 {
			// nothing found or a gap
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "filters doesn't contains primary key fields")
		}
		if len(repeatedFields) > 1 && parent == AndOP {
			// with AND there is no use of EQ on the same field
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "reusing same fields for conditions on equality")
		}

		compositeKeys[0] = append(compositeKeys[0], repeatedFields[0])
		// as we found some repeated fields in the filter so clone the first set of keys and add this prefix to all the
		// repeated fields, cloning is only needed if there are more than one repeated fields
		for j := 1; j < len(repeatedFields); j++ {
			keyPartsCopy := make([]*Selector, len(compositeKeys[0])-1)
			copy(keyPartsCopy, compositeKeys[0][0:len(compositeKeys[0])-1])
			keyPartsCopy = append(keyPartsCopy, repeatedFields[j])
			compositeKeys = append(compositeKeys, keyPartsCopy)
		}
	}

	// keys building is dependent on the filter type
	var allKeys []keys.Key
	for _, k := range compositeKeys {
		switch parent {
		case AndOP:
			var primaryKeyParts []interface{}
			for _, s := range k {
				primaryKeyParts = append(primaryKeyParts, s.Matcher.GetValue().AsInterface())
			}

			key, err := s.keyEncodingFunc(primaryKeyParts...)
			if err != nil {
				return nil, err
			}
			allKeys = append(allKeys, key)
		case OrOP:
			for _, sel := range k {
				if len(userDefinedKeys) > 1 {
					// this means OR can't build independently these keys
					return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "OR is not supported with composite primary keys")
				}

				key, err := s.keyEncodingFunc(sel.Matcher.GetValue().AsInterface())
				if err != nil {
					return nil, err
				}

				allKeys = append(allKeys, key)
			}
		}
	}

	return allKeys, nil
}
