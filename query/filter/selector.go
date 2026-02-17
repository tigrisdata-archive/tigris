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
	"encoding/json"
	"fmt"
	"math"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/date"
	"github.com/tigrisdata/tigris/schema"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

// Selector is a condition defined inside a filter. It has a field which corresponding the field on which condition
// is defined and then Matcher. A matcher is formed from the user condition i.e. if the condition is on "$eq" then
// a EqualityMatcher is created. The matcher implements a Match method which is used to know if the input document
// passes the condition.
//
// A Selector can have this form inside the input JSON
//
//	{f:{$eq:1}}
//	{f:20} (default is "$eq" so we automatically append EqualityMatcher for this case in parser)
//	{f:<Expr>}
type Selector struct {
	Matcher   ValueMatcher
	Parent    *schema.QueryableField
	Field     *schema.QueryableField
	Collation *value.Collation
}

// NewSelector returns Selector object.
func NewSelector(parent *schema.QueryableField, field *schema.QueryableField, matcher ValueMatcher, collation *value.Collation) *Selector {
	return &Selector{
		Parent:    parent,
		Field:     field,
		Matcher:   matcher,
		Collation: collation,
	}
}

func (s *Selector) MatchesDoc(doc map[string]any) bool {
	// we don't need any special handling for arrays in this case because this is
	// already taken care by search store
	v, ok := doc[s.Field.Name()]
	if !ok {
		return true
	}

	var val value.Value
	switch s.Field.DataType {
	case schema.StringType:
		// there can be special characters in the string, we need to perform exact match.
		val = value.NewStringValue(v.(string), s.Collation)
	case schema.DoubleType:
		// this method is only used with indexing store and it returns `json.Number` for all numeric types
		var err error
		val, err = value.NewDoubleValue(v.(json.Number).String())
		if ulog.E(err) {
			return true
		}
	default:
		// as this method is only intended for indexing store, so we only apply filter for string and double type
		// otherwise we rely on indexing store to only return valid results.
		return true
	}

	return s.Matcher.Matches(val)
}

// Matches returns true if the input doc matches this filter.
// To note around the order of checking for not exist and error logging
// An error is returned if the field does not exist
// and that is an acceptable error, so return false
// Only log an error that is unexpected.
func (s *Selector) Matches(doc []byte, metadata []byte) bool {
	if ((s.Parent != nil && s.Parent.DataType == schema.ArrayType) || (s.Field.DataType == schema.ArrayType)) && !MatcherForArray(s.Matcher) {
		// The second condition is on matcher i.e. if filter has received an array then we don't need
		// "ArrMatches" comparison because then "Matches" can simply compare both the arrays. Here we
		// are handling case when user is passing non-array value in a filter for a field which is
		// an array which means we need to iterate on an array and perform the comparison. This is the
		// responsibility of ArrMatches method.
		// We handle here,
		// - filtering inside array of objects
		// - single element comparison inside array
		// - single element range on an array
		arr, err := s.getArrayField(doc)
		if ulog.E(err) {
			return false
		}
		return s.Matcher.ArrMatches(arr)
	}

	docValue, dtp, err := getJSONField(doc, metadata, s.Field.FieldName, s.Field.KeyPath())
	if dtp == jsonparser.NotExist {
		return false
	}
	if ulog.E(err) {
		return false
	}

	if dtp == jsonparser.Null {
		docValue = nil
	}

	var val value.Value
	if s.Collation != nil {
		val, err = value.NewValueUsingCollation(s.Field.DataType, docValue, s.Collation)
	} else {
		val, err = value.NewValue(s.Field.DataType, docValue)
	}
	if ulog.E(err) {
		return false
	}

	return s.Matcher.Matches(val)
}

func (s *Selector) ToSearchFilter() string {
	var op string
	switch s.Matcher.Type() {
	case EQ:
		op = "%s:=%v"
	case GT:
		op = "%s:>%v"
	case GTE:
		op = "%s:>=%v"
	case LT:
		op = "%s:<%v"
	case LTE:
		op = "%s:<=%v"
	}

	v := s.Matcher.GetValue()
	switch s.Field.DataType {
	case schema.DoubleType:
		// for double, we pass string in the filter to search backend
		return fmt.Sprintf(op, s.Field.InMemoryName(), v.String())
	case schema.DateTimeType:
		// encode into int64
		if nsec, err := date.ToUnixNano(schema.DateTimeFormat, v.String()); err == nil {
			return fmt.Sprintf(op, s.Field.InMemoryName(), nsec)
		}
	case schema.ArrayType:
		if _, ok := v.(*value.ArrayValue); ok {
			var filterString string
			for i, item := range v.AsInterface().([]any) {
				if i != 0 {
					filterString += "&&"
				}
				filterString += fmt.Sprintf(op, s.Field.InMemoryName(), item)
			}
			return filterString
		}
	case schema.StringType:
		return fmt.Sprintf(op, s.Field.InMemoryName(), fmt.Sprintf("`%s`", v.AsInterface()))
	}
	return fmt.Sprintf(op, s.Field.InMemoryName(), v.AsInterface())
}

func (s *Selector) IsSearchIndexed() bool {
	if !s.Field.SearchIndexed {
		return false
	}

	switch {
	case s.Field.DataType == schema.DoubleType:
		v, ok := s.Matcher.GetValue().(*value.DoubleValue)
		if !ok {
			return false
		}

		if v.Double < value.SmallestNonZeroNormalFloat32 && v.Double > -value.SmallestNonZeroNormalFloat32 {
			return false
		}

		return v.Double < math.MaxFloat32 && v.Double > -math.MaxFloat32
	default:
		return !(s.Field.DataType == schema.ByteType || s.Matcher.GetValue().AsInterface() == nil)
	}
}

// String a helpful method for logging.
func (s *Selector) String() string {
	return fmt.Sprintf("{%v:%v}", s.Field.Name(), s.Matcher)
}

// getArrayField is to extract an array from doc and then extract fieldName from each element of this Array. In case
// element is not object then it simply returns the array.
func (s *Selector) getArrayField(doc []byte) ([]any, error) {
	keyPath := s.Field.KeyPath()
	if s.Parent != nil {
		// this means we need to fetch parent array
		keyPath = s.Parent.KeyPath()
	}

	var items []any
	var err error
	_, err = jsonparser.ArrayEach(doc, func(item []byte, vt jsonparser.ValueType, offset int, err1 error) {
		if err1 != nil {
			err = err1
			return
		}

		var converted any
		if vt == jsonparser.Object {
			// if array of object then we try to "Get" the requested field
			v, dtp, _, er := jsonparser.Get(item, s.Field.UnFlattenName)
			if dtp == jsonparser.NotExist || er != nil {
				err = er
				return
			}

			converted, err = convertByteToType(s.Field.SubType, dtp, v)
		} else {
			converted, err = convertByteToType(s.Field.SubType, vt, item)
		}
		if err != nil {
			return
		}

		items = append(items, converted)
	}, keyPath...)

	return items, err
}

func convertByteToType(ft schema.FieldType, dtp jsonparser.ValueType, value []byte) (any, error) {
	if dtp == jsonparser.Array || ft == schema.ArrayType {
		var arr []any
		if err := jsoniter.Unmarshal(value, &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}

	switch ft {
	case schema.DoubleType:
		return jsonparser.ParseFloat(value)
	case schema.StringType, schema.UUIDType:
		return string(value), nil
	case schema.Int32Type, schema.Int64Type:
		return jsonparser.ParseInt(value)
	case schema.BoolType:
		return jsonparser.ParseBoolean(value)
	case schema.NullType:
		return value, nil
	}

	switch dtp {
	case jsonparser.Boolean:
		return jsonparser.ParseBoolean(value)
	case jsonparser.String:
		return string(value), nil
	case jsonparser.Number:
		return jsonparser.ParseFloat(value)
	case jsonparser.Null:
		return value, nil
	}

	return nil, errors.InvalidArgument("unsupported value found '%s'", string(value))
}

func getJSONField(doc []byte, metadata []byte, fieldName string, keyPath []string) ([]byte, jsonparser.ValueType, error) {
	var docValue []byte
	var dtp jsonparser.ValueType
	var err error

	if schema.IsReservedField(fieldName) {
		docValue, dtp, _, err = jsonparser.Get(metadata, keyPath...)
	} else {
		docValue, dtp, _, err = jsonparser.Get(doc, keyPath...)
	}

	return docValue, dtp, err
}
