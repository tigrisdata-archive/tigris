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
	"sort"

	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	tsort "github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/value"
)

// KeyComposer needs to be implemented to have a custom Compose method with different constraints.
type KeyComposer[F fieldable] interface {
	Compose(level []*Selector, userDefinedKeys []F, parent LogicalOP) ([]QueryPlan, error)
}

type (
	KeyEncodingFunc     func(indexParts ...interface{}) (keys.Key, error)
	BuildIndexPartsFunc func(fieldName string, val value.Value) []interface{}
)

type IndexType uint8

const (
	Unknown IndexType = iota
	PrimaryIndex
	SecondaryIndex
)

type QueryPlanType uint8

const (
	EQUAL QueryPlanType = iota
	RANGE
	FULLRANGE
)

func IndexTypeSecondary(indexType IndexType) bool {
	return indexType == SecondaryIndex
}

func IndexTypePrimary(indexType IndexType) bool {
	return indexType == PrimaryIndex
}

type TableScanPlan struct {
	Table   []byte
	From    keys.Key
	Reverse bool
}

// QueryPlan is returned by KeyBuilder that contains the keys and type of query against fdb.
type QueryPlan struct {
	QueryType           QueryPlanType
	FieldName           string
	DataType            schema.FieldType
	Keys                []keys.Key
	Ascending           bool
	IndexType           IndexType
	From                keys.Key
}

func NewQueryPlan(queryType QueryPlanType, fieldName string, dataType schema.FieldType, keys []keys.Key, indexType IndexType) QueryPlan {
	return QueryPlan{
		QueryType: queryType,
		FieldName: fieldName,
		DataType:  dataType,
		Keys:      keys,
		Ascending: true,
		IndexType: indexType,
	}
}

func (q QueryPlan) Reverse() bool {
	return !q.Ascending
}

// SortQueryPlans creates a simple way to choose a best query plan
// based on the queryType.
func SortQueryPlans(queries []QueryPlan) []QueryPlan {
	sort.Slice(queries, func(i, j int) bool {
		return queries[i].DataType < queries[j].DataType
	})
	return queries
}

func (q QueryPlan) GetKeyInterfaceParts() [][]interface{} {
	keys := make([][]interface{}, len(q.Keys))
	for i, key := range q.Keys {
		keys[i] = key.IndexParts()
	}
	return keys
}

type fieldable interface {
	Name() string
	Type() schema.FieldType
}

// KeyBuilder is responsible for building internal Keys. A composer is caller by the builder to build the internal keys
// based on the Composer logic.
// KeyBuilder uses generics so that it can accept either schema.QueryableField or schema.Field
// so that it can build a query plan for primay or secondary indexes.
type KeyBuilder[F fieldable] struct {
	composer  KeyComposer[F]
	indexType IndexType
}

// NewPrimaryKeyEqBuilder returns a KeyBuilder for use with schema.Field to build a primary key query plan.
func NewPrimaryKeyEqBuilder(keyEncodingFunc KeyEncodingFunc) *KeyBuilder[*schema.Field] {
	return NewKeyBuilder[*schema.Field](
		NewStrictEqKeyComposer[*schema.Field](keyEncodingFunc, PKBuildIndexPartsFunc, true, PrimaryIndex),
		PrimaryIndex,
	)
}

// NewSecondaryKeyEqBuilder returns a KeyBuilder for use with the secondary index.
func NewSecondaryKeyEqBuilder[F fieldable](keyEncodingFunc KeyEncodingFunc, buildIndexPartsFunc BuildIndexPartsFunc) *KeyBuilder[F] {
	return NewKeyBuilder[F](
		NewStrictEqKeyComposer[F](keyEncodingFunc, buildIndexPartsFunc, false, SecondaryIndex),
		SecondaryIndex,
	)
}

// NewRangeKeyBuilder returns a KeyBuilder for use with schema.QueryableField.
func NewRangeKeyBuilder(composer KeyComposer[*schema.QueryableField], indexType IndexType) *KeyBuilder[*schema.QueryableField] {
	return &KeyBuilder[*schema.QueryableField]{
		composer:  composer,
		indexType: indexType,
	}
}

// NewKeyBuilder returns a KeyBuilder.
func NewKeyBuilder[F fieldable](composer KeyComposer[F], indexType IndexType) *KeyBuilder[F] {
	return &KeyBuilder[F]{
		composer:  composer,
		indexType: indexType,
	}
}

// Build is responsible for building the internal keys from the user filter and using the keys defined in the schema
// and passed by the caller in this method. The build is doing a level by level traversal to build the internal Keys.
// On each level multiple keys can be formed because the user can specify ranges. The builder is not deciding the logic
// of key generation, the builder is simply traversing on the filters and calling compose where the logic resides.
// If the build is for a primary key, the query plans are merged into a single plan.
func (k *KeyBuilder[F]) Build(filters []Filter, userDefinedKeys []F) ([]QueryPlan, error) {
	var queue []Filter
	var singleLevel []*Selector
	var allKeys []QueryPlan
	for _, f := range filters {
		switch ss := f.(type) {
		case *Selector:
			singleLevel = append(singleLevel, ss)
		case LogicalFilter:
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
			if e.Type() == OrOP && IndexTypeSecondary(k.indexType) {
				return nil, errors.InvalidArgument("$or filter is not yet supported for secondary index")
			}
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

	// PrimaryKey is always a single plan with all the keys
	if IndexTypePrimary(k.indexType) {
		combined := allKeys[0]
		for _, plan := range allKeys[1:] {
			combined.Keys = append(combined.Keys, plan.Keys...)
		}
		return []QueryPlan{combined}, nil
	}

	return allKeys, nil
}

func PKBuildIndexPartsFunc(name string, value value.Value) []interface{} {
	return []interface{}{value.AsInterface()}
}

// StrictEqKeyComposer works in to ways to generate internal keys if the condition is equality.
//  1. When `matchAll=true`, it will generate internal keys of equality on the fields of the schema if all these fields
//     are present in the filters. The following rules are applied for StrictEqKeyComposer:
//     - The userDefinedKeys(indexes defined in the schema) passed in parameter should be present in the filter
//     - For AND filters it is possible to build internal keys for composite indexes, for OR it is not possible.
//
// 2. When `matchAll=false`, it will treat all userDefined as individual and generate an `$eq` query plan for each one that is found.
//
// For OR filter an error is returned if it is used for indexes that are composite.
type StrictEqKeyComposer[F fieldable] struct {
	matchAll bool
	// keyEncodingFunc returns encoded key from index parts
	keyEncodingFunc     KeyEncodingFunc
	buildIndexPartsFunc BuildIndexPartsFunc
	indexType           IndexType
}

func NewStrictEqKeyComposer[F fieldable](keyEncodingFunc KeyEncodingFunc, buildIndexPartsFunc BuildIndexPartsFunc, matchAll bool, indexType IndexType) *StrictEqKeyComposer[F] {
	return &StrictEqKeyComposer[F]{
		matchAll,
		keyEncodingFunc,
		buildIndexPartsFunc,
		indexType,
	}
}

// Compose is implementing the logic of composing keys.
func (s *StrictEqKeyComposer[F]) Compose(selectors []*Selector, userDefinedKeys []F, parent LogicalOP) ([]QueryPlan, error) {
	var compositeKeys [][]*Selector
	if s.matchAll {
		compositeKeys = make([][]*Selector, 1) // allocate just for the first keyParts
	}
	for _, k := range userDefinedKeys {
		var repeatedFields []*Selector
		for _, sel := range selectors {
			if sel.Matcher.Type() == EQ {
				if k.Name() == sel.Field.Name() {
					repeatedFields = append(repeatedFields, sel)
				}
			} else if s.matchAll {
				return nil, errors.InvalidArgument("filters only supporting $eq comparison, found '%s'", sel.Matcher.Type())
			}
		}

		if len(repeatedFields) == 0 {
			if s.matchAll {
				// nothing found or a gap
				return nil, errors.InvalidArgument("filters doesn't contains primary key fields")
			} else {
				continue
			}
		}
		if len(repeatedFields) > 1 && parent == AndOP && s.matchAll {
			// with AND there is no use of EQ on the same field
			return nil, errors.InvalidArgument("reusing same fields for conditions on equality")
		}

		if s.matchAll {
			compositeKeys[0] = append(compositeKeys[0], repeatedFields[0])
			// as we found some repeated fields in the filter so clone the first set of keys and add this prefix to all the
			// repeated fields, cloning is only needed if there are more than one repeated fields
			for j := 1; j < len(repeatedFields); j++ {
				keyPartsCopy := make([]*Selector, len(compositeKeys[0])-1)
				copy(keyPartsCopy, compositeKeys[0][0:len(compositeKeys[0])-1])
				keyPartsCopy = append(keyPartsCopy, repeatedFields[j]) //nolint:makezero
				compositeKeys = append(compositeKeys, keyPartsCopy)    //nolint:makezero
			}
		} else {
			compositeKeys = append(compositeKeys, [][]*Selector{repeatedFields}...) //nolint:makezero
		}
	}

	// keys building is dependent on the filter type
	var queryPlans []QueryPlan
	for _, k := range compositeKeys {
		switch parent {
		case AndOP:
			var keyParts []interface{}
			for _, sel := range k {
				newParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Matcher.GetValue())
				keyParts = append(keyParts, newParts...)
			}

			key, err := s.keyEncodingFunc(keyParts...)
			if err != nil {
				return nil, err
			}
			dataType := schema.UnknownType
			fieldName := ""
			if len(k) == 1 {
				dataType = k[0].Field.DataType
				fieldName = k[0].Field.Name()
			}
			queryPlans = append(queryPlans, NewQueryPlan(EQUAL, fieldName, dataType, []keys.Key{key}, s.indexType))
		case OrOP:
			for _, sel := range k {
				if len(userDefinedKeys) > 1 {
					// this means OR can't build independently these keys
					return nil, errors.InvalidArgument("OR is not supported with composite primary keys")
				}

				primaryKeyParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Matcher.GetValue())

				key, err := s.keyEncodingFunc(primaryKeyParts...)
				if err != nil {
					return nil, err
				}

				queryPlans = append(queryPlans, NewQueryPlan(EQUAL, sel.Field.Name(), sel.Field.DataType, []keys.Key{key}, s.indexType))
			}
		}
	}

	return queryPlans, nil
}

// RangeKeyComposer will generate a range key set on the user defined keys
// It will set the KeyQuery to `FullRange` if the start or end key is not defined in the query
// if there is a defined start and end key for a range then `Range` is set.
type RangeKeyComposer[F fieldable] struct {
	// keyEncodingFunc returns encoded key from index parts
	keyEncodingFunc     KeyEncodingFunc
	buildIndexPartsFunc BuildIndexPartsFunc
	indexType           IndexType
}

func NewRangeKeyComposer[F fieldable](keyEncodingFunc KeyEncodingFunc, buildIndexParts BuildIndexPartsFunc, indexType IndexType) *RangeKeyComposer[F] {
	return &RangeKeyComposer[F]{
		keyEncodingFunc,
		buildIndexParts,
		indexType,
	}
}

func (s *RangeKeyComposer[F]) Compose(selectors []*Selector, userDefinedKeys []F, parent LogicalOP) ([]QueryPlan, error) {
	var err error
	var queryPlans []QueryPlan
	for _, k := range userDefinedKeys {
		var begin, end keys.Key
		rangeType := FULLRANGE
		for _, sel := range selectors {
			if k.Name() == sel.Field.Name() && s.isRange(sel) {
				indexParts := s.buildIndexPartsFunc(sel.Field.Name(), sel.Matcher.GetValue())
				if s.isGreater(sel) {
					if sel.Matcher.Type() == GT {
						indexParts = append(indexParts, 0xFF)
					}

					begin, err = s.keyEncodingFunc(indexParts...)
					if err != nil {
						return nil, err
					}

					if end == nil {
						lessIndexParts := s.buildIndexPartsFunc(sel.Field.Name(), value.MaxOrderValue())
						end, err = s.keyEncodingFunc(lessIndexParts...)
						if err != nil {
							return nil, err
						}
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
						greaterIndexParts := s.buildIndexPartsFunc(sel.Field.Name(), value.MinOrderValue())
						begin, err = s.keyEncodingFunc(greaterIndexParts...)
						if err != nil {
							return nil, err
						}
					} else {
						rangeType = RANGE
					}
				}
			}
		}

		if begin != nil && end != nil {
			queryPlans = append(queryPlans, NewQueryPlan(rangeType, k.Name(), k.Type(), []keys.Key{begin, end}, s.indexType))
		}
	}

	if len(queryPlans) == 0 {
		return nil, errors.InvalidArgument("No range query found")
	}
	return queryPlans, nil
}

func (s *RangeKeyComposer[F]) isRange(selector *Selector) bool {
	if s.isGreater(selector) || s.isLess(selector) {
		return true
	}
	return false
}

func (s *RangeKeyComposer[F]) isGreater(selector *Selector) bool {
	switch selector.Matcher.Type() {
	case GT, GTE:
		return true
	default:
		return false
	}
}

func (s *RangeKeyComposer[F]) isLess(selector *Selector) bool {
	switch selector.Matcher.Type() {
	case LT, LTE:
		return true
	default:
		return false
	}
}

func QueryPlanFromSort(sortFields *[]tsort.SortField, indexableFields []*schema.QueryableField, encoder KeyEncodingFunc, buildIndexParts BuildIndexPartsFunc, indexType IndexType) (*QueryPlan, error) {
	if sortFields == nil {
		return nil, nil
	}

	if len(*sortFields) == 0 {
		return nil, nil
	}

	var field *schema.QueryableField
	for _, idx := range indexableFields {
		if idx.FieldName == (*sortFields)[0].Name {
			field = idx

			break
		}
	}

	if field == nil {
		return nil, errors.InvalidArgument("Sort field is not indexed")
	}

	min, err := encoder(buildIndexParts(field.FieldName, value.MinOrderValue())...)
	if err != nil {
		return nil, err
	}

	max, err := encoder(buildIndexParts(field.FieldName, value.MaxOrderValue())...)
	if err != nil {
		return nil, err
	}

	plan := &QueryPlan{
		FieldName:           field.FieldName,
		QueryType:           FULLRANGE,
		DataType:            field.DataType,
		Ascending:           (*sortFields)[0].Ascending,
		Keys:                []keys.Key{min, max},
		IndexType:           indexType,
	}

	return plan, nil
}
