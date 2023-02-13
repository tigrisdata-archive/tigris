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

package filter

import (
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/value"
)

const (
	RANGE     string = "range"
	FULLRANGE string = "full_range"
	EQUAL     string = "equal"
)

// The KeyBuilder returns a KeyQuery which can be used to determine the type of range read
type KeyQuery struct {
	QueryType string
	DataType  schema.FieldType
	Keys      []keys.Key
}

func newKeyQuery(queryType string, dataType schema.FieldType, keys []keys.Key) KeyQuery {
	return KeyQuery{
		queryType,
		dataType,
		keys,
	}
}

// KeyBuilder is responsible for building internal Keys. A composer is caller by the builder to build the internal keys
// based on the Composer logic.
type KeyBuilder struct {
	composer KeyComposer
}

// NewKeyBuilder returns a KeyBuilder.
func NewKeyBuilder(composer KeyComposer) *KeyBuilder {
	return &KeyBuilder{
		composer: composer,
	}
}

func (k *KeyBuilder) BuildFromFields(filters []Filter, userDefinedKeys []*schema.Field) ([]KeyQuery, error) {
	queryable := schema.BuildQueryableFields(userDefinedKeys, nil)
	return k.Build(filters, queryable)
}

// Build is responsible for building the internal keys from the user filter and using the keys defined in the schema
// and passed by the caller in this method. The build is doing a level by level traversal to build the internal Keys.
// On each level multiple keys can be formed because the user can specify ranges. The builder is not deciding the logic
// of key generation, the builder is simply traversing on the filters and calling compose where the logic resides.
func (k *KeyBuilder) Build(filters []Filter, userDefinedKeys []*schema.QueryableField) ([]KeyQuery, error) {
	var queue []Filter
	var singleLevel []*Selector
	var allKeys []KeyQuery
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

func PKBuildIndexPartsFunc(name string, datatype schema.FieldType, value interface{}) []interface{} {
	return []interface{}{value}
}

// KeyComposer needs to be implemented to have a custom Compose method with different constraints.
type KeyComposer interface {
	Compose(level []*Selector, userDefinedKeys []*schema.QueryableField, parent LogicalOP) ([]KeyQuery, error)
}

type KeyEncodingFunc func(indexParts ...interface{}) (keys.Key, error)
type BuildIndexPartsFunc func(fieldName string, datatype schema.FieldType, value interface{}) []interface{}

// StrictEqKeyComposer is to generate internal keys for the only if the condition is equality on the fields that are part of
// the schema and all these fields are present in the filters. The following rules are applied for StrictEqKeyComposer
//   - The userDefinedKeys(indexes defined in the schema) passed in parameter should be present in the filter
//   - For AND filters it is possible to build internal keys for composite indexes, for OR it is not possible.
//
// So for OR filter an error is returned if it is used for indexes that are composite.
type StrictEqKeyComposer struct {
	// keyEncodingFunc returns encoded key from index parts
	keyEncodingFunc     KeyEncodingFunc
	buildIndexPartsFunc BuildIndexPartsFunc
}

func NewStrictEqKeyComposer(keyEncodingFunc KeyEncodingFunc, buildIndexPartsFunc BuildIndexPartsFunc) *StrictEqKeyComposer {
	return &StrictEqKeyComposer{
		keyEncodingFunc,
		buildIndexPartsFunc,
	}
}

func (s *StrictEqKeyComposer) startIndexParts(name string, value interface{}) []interface{} {
	return []interface{}{}
}

// Compose is implementing the logic of composing keys.
func (s *StrictEqKeyComposer) Compose(selectors []*Selector, userDefinedKeys []*schema.QueryableField, parent LogicalOP) ([]KeyQuery, error) {
	compositeKeys := make([][]*Selector, 1) // allocate just for the first keyParts
	for _, k := range userDefinedKeys {
		var repeatedFields []*Selector
		for _, sel := range selectors {
			if k.FieldName == sel.Field.Name() {
				repeatedFields = append(repeatedFields, sel)
			}
			if sel.Matcher.Type() != EQ {
				return nil, errors.InvalidArgument("filters only supporting $eq comparison, found '%s'", sel.Matcher.Type())
			}
		}

		if len(repeatedFields) == 0 {
			// nothing found or a gap
			return nil, errors.InvalidArgument("filters doesn't contains primary key fields")
		}
		if len(repeatedFields) > 1 && parent == AndOP {
			// with AND there is no use of EQ on the same field
			return nil, errors.InvalidArgument("reusing same fields for conditions on equality")
		}

		compositeKeys[0] = append(compositeKeys[0], repeatedFields[0])
		// as we found some repeated fields in the filter so clone the first set of keys and add this prefix to all the
		// repeated fields, cloning is only needed if there are more than one repeated fields
		for j := 1; j < len(repeatedFields); j++ {
			keyPartsCopy := make([]*Selector, len(compositeKeys[0])-1)
			copy(keyPartsCopy, compositeKeys[0][0:len(compositeKeys[0])-1])
			keyPartsCopy = append(keyPartsCopy, repeatedFields[j]) //nolint:makezero
			compositeKeys = append(compositeKeys, keyPartsCopy)    //nolint:makezero
		}
	}

	// keys building is dependent on the filter type
	var allKeys []KeyQuery
	for _, k := range compositeKeys {
		switch parent {
		case AndOP:
			var keyParts []interface{}
			for _, sel := range k {

				newParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Field.DataType, sel.Matcher.GetValue().AsInterface())
				keyParts = append(keyParts, newParts...)
			}

			eqkey, err := s.keyEncodingFunc(keyParts...)
			if err != nil {
				return nil, err
			}
			key := newKeyQuery(EQUAL, schema.UnknownType, []keys.Key{eqkey})
			allKeys = append(allKeys, key)
		case OrOP:
			for _, sel := range k {
				if len(userDefinedKeys) > 1 {
					// this means OR can't build independently these keys
					return nil, errors.InvalidArgument("OR is not supported with composite primary keys")
				}

				primaryKeyParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Field.DataType, sel.Matcher.GetValue().AsInterface())

				eqkey, err := s.keyEncodingFunc(primaryKeyParts...)
				if err != nil {
					return nil, err
				}

				key := newKeyQuery(EQUAL, schema.UnknownType, []keys.Key{eqkey})
				allKeys = append(allKeys, key)
			}
		}
	}

	return allKeys, nil
}

// Range Key Composer will generate a range key set on the user defined keys
// It will set the KeyQuery to `FullRange` if the start or end key is not defined in the query
// if there is a defined start and end key for a range then `Range` is set
type RangeKeyComposer struct {
	// keyEncodingFunc returns encoded key from index parts
	keyEncodingFunc     KeyEncodingFunc
	buildIndexPartsFunc BuildIndexPartsFunc
}

func NewRangeKeyComposer(keyEncodingFunc KeyEncodingFunc, buildIndexParts BuildIndexPartsFunc) *RangeKeyComposer {
	return &RangeKeyComposer{
		keyEncodingFunc,
		buildIndexParts,
	}
}

func (s *RangeKeyComposer) Compose(selectors []*Selector, userDefinedKeys []*schema.QueryableField, parent LogicalOP) ([]KeyQuery, error) {
	var err error
	var foundKeys []KeyQuery
	for _, k := range userDefinedKeys {
		var begin, end keys.Key
		rangeType := FULLRANGE
		for _, sel := range selectors {
			if k.FieldName == sel.Field.Name() && s.isRange(sel) {
				indexParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Field.DataType, sel.Matcher.GetValue().AsInterface())
				if s.isGreater(sel) {
					if sel.Matcher.Type() == GT {
						indexParts = append(indexParts, 0xFF)
					}

					begin, err = s.keyEncodingFunc(indexParts...)
					if err != nil {
						return nil, err
					}

					if end == nil {
						lessIndexParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Field.DataType, value.Max(sel.Field.DataType, sel.Matcher.GetValue()))
						end, err = s.keyEncodingFunc(lessIndexParts...)
					} else {
						rangeType = RANGE
					}
				} else {
					if sel.Matcher.Type() == LTE {
						indexParts = append(indexParts, 0xFF)
					}

					end, err = s.keyEncodingFunc(indexParts...)
					if err != nil {
						return nil, err
					}

					if begin == nil {
						greaterIndexParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Field.DataType, value.Min(sel.Field.DataType, sel.Matcher.GetValue()))
						begin, err = s.keyEncodingFunc(greaterIndexParts...)
					} else {
						rangeType = RANGE
					}
				}
			}
		}

		if begin != nil && end != nil {
			foundKeys = append(foundKeys, newKeyQuery(rangeType, k.DataType, []keys.Key{begin, end}))
		}
	}

	if len(foundKeys) == 0 {
		return nil, errors.InvalidArgument("No range query found")
	}

	return foundKeys, nil

}

func (s *RangeKeyComposer) isRange(selector *Selector) bool {
	if s.isGreater(selector) || s.isLess(selector) {
		return true
	}

	return false
}

func (s *RangeKeyComposer) isGreater(selector *Selector) bool {
	switch selector.Matcher.Type() {
	case GT, GTE:
		return true

	default:
		return false
	}
}

func (s *RangeKeyComposer) isLess(selector *Selector) bool {
	switch selector.Matcher.Type() {
	case LT, LTE:
		return true

	default:
		return false
	}
}
