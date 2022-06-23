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
	"bytes"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/query/expression"
	"github.com/tigrisdata/tigris/schema"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
)

var (
	filterAll   = []byte(`{}`)
	emptyFilter = &WrappedFilter{Filter: &EmptyFilter{}}
)

// A Filter represents a query filter that can have any multiple conditions, logical filtering, nested conditions, etc
// On a high level, a filter from a user query will map like this
//    {Selector} --> Filter with a single condition
//    {Selector, Selector, LogicalOperator} --> Filter with two condition and a logicalOperator
//    {Selector, LogicalOperator} --> Filter with single condition and a logicalOperator
//    and so on...
//
// The JSON representation for these filters will look like below,
// "filter: {"f1": 10}
// "filter": [{"f1": 10}, {"f2": {"$gt": 10}}]
// "filter": [{"f1": 10}, {"f2": 10}, {"$or": [{"f3": 20}, {"$and": [{"f4":5}, {"f5": 6}]}]}]
//
// The default rule applied between filters are "$and and the default selector is "$eq".
type Filter interface {
	// Matches returns true if the input doc passes the filter, otherwise false
	Matches(doc []byte) bool
	// MatchesDoc similar to Matches but used when document is already parsed
	MatchesDoc(doc map[string]interface{}) bool
	ToSearchFilter() string
}

type EmptyFilter struct{}

func (f *EmptyFilter) Matches(_ []byte) bool                    { return true }
func (f *EmptyFilter) MatchesDoc(_ map[string]interface{}) bool { return true }
func (f *EmptyFilter) ToSearchFilter() string                   { return "" }

type WrappedFilter struct {
	Filter Filter
}

func NewWrappedFilter(filters []Filter) *WrappedFilter {
	if len(filters) == 0 {
		return emptyFilter
	}

	if len(filters) <= 1 {
		return &WrappedFilter{
			Filter: filters[0],
		}
	}

	return &WrappedFilter{
		Filter: &AndFilter{
			filter: filters,
		},
	}
}

func All(reqFilter []byte) bool {
	return len(reqFilter) == 0 || bytes.Equal(reqFilter, filterAll)
}

type Factory struct {
	fields []*schema.Field
}

func NewFactory(fields []*schema.Field) *Factory {
	return &Factory{
		fields: fields,
	}
}

func (factory *Factory) WrappedFilter(reqFilter []byte) (*WrappedFilter, error) {
	filters, err := factory.Factorize(reqFilter)
	if err != nil {
		return nil, err
	}

	return NewWrappedFilter(filters), nil
}

func (factory *Factory) Factorize(reqFilter []byte) ([]Filter, error) {
	if len(reqFilter) == 0 {
		return nil, nil
	}

	var seen = make(map[string]struct{})
	var filters []Filter
	var err error
	err = jsonparser.ObjectEach(reqFilter, func(k []byte, v []byte, jsonDataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		var filter Filter
		switch string(k) {
		case string(AndOP):
			filter, err = factory.UnmarshalAnd(v)
		case string(OrOP):
			filter, err = factory.UnmarshalOr(v)
		default:
			filter, err = factory.ParseSelector(k, v, jsonDataType)
		}
		if err != nil {
			return err
		}
		if _, ok := seen[string(k)]; ok {
			return api.Errorf(api.Code_INVALID_ARGUMENT, "duplicate filter '%s'", string(k))
		}
		seen[string(k)] = struct{}{}
		filters = append(filters, filter)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return filters, nil
}

func (factory *Factory) UnmarshalFilter(input jsoniter.RawMessage) (expression.Expr, error) {
	var err error
	var filter Filter
	err = jsonparser.ObjectEach(input, func(k []byte, v []byte, jsonDataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch string(k) {
		case string(AndOP):
			filter, err = factory.UnmarshalAnd(v)
		case string(OrOP):
			filter, err = factory.UnmarshalOr(v)
		default:
			filter, err = factory.ParseSelector(k, v, jsonDataType)
		}
		return nil
	})

	return filter, err
}

func (factory *Factory) UnmarshalAnd(input jsoniter.RawMessage) (Filter, error) {
	expr, err := expression.UnmarshalArray(input, factory.UnmarshalFilter)
	if err != nil {
		return nil, err
	}
	andFilters, err := convertExprListToFilters(expr)
	if err != nil {
		return nil, err
	}

	return NewAndFilter(andFilters)
}

func (factory *Factory) UnmarshalOr(input jsoniter.RawMessage) (Filter, error) {
	expr, err := expression.UnmarshalArray(input, factory.UnmarshalFilter)
	if err != nil {
		return nil, err
	}
	orFilters, err := convertExprListToFilters(expr)
	if err != nil {
		return nil, err
	}

	return NewOrFilter(orFilters)
}

func convertExprListToFilters(expr []expression.Expr) ([]Filter, error) {
	var filters []Filter
	for _, e := range expr {
		f, ok := e.(Filter)
		if !ok {
			return nil, ulog.CE("not able to decode to filter %v", f)
		}
		filters = append(filters, f)
	}

	return filters, nil
}

// ParseSelector is a short-circuit for Selector i.e. when we know the filter passed is not logical then we directly
// call this because if it is not logical then it is simply a Selector filter.
func (factory *Factory) ParseSelector(k []byte, v []byte, dataType jsonparser.ValueType) (Filter, error) {
	var field *schema.Field
	for _, f := range factory.fields {
		if f.FieldName == string(k) {
			field = f
		}
	}
	if field == nil {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "querying on non schema field '%s'", string(k))
	}

	switch dataType {
	case jsonparser.Boolean, jsonparser.Number, jsonparser.String:
		val, err := value.NewValue(field.DataType, v)
		if err != nil {
			return nil, err
		}

		return NewSelector(field, NewEqualityMatcher(val)), nil
	case jsonparser.Object:
		valueMatcher, err := buildComparisonOperator(v, field)
		if err != nil {
			return nil, err
		}

		return NewSelector(field, valueMatcher), nil
	default:
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unable to parse the comparison operator")
	}
}

func buildComparisonOperator(input jsoniter.RawMessage, field *schema.Field) (ValueMatcher, error) {
	if len(input) == 0 {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "empty object")
	}

	var valueMatcher ValueMatcher
	var err error
	err = jsonparser.ObjectEach(input, func(key []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch string(key) {
		case EQ, GT, GTE, LT, LTE:
			switch dataType {
			case jsonparser.Boolean, jsonparser.Number, jsonparser.String, jsonparser.Null:
				var val value.Value
				val, err = value.NewValue(field.DataType, v)
				if err != nil {
					return err
				}

				valueMatcher, err = NewMatcher(string(key), val)
				return err
			}
		default:
			return api.Errorf(api.Code_INVALID_ARGUMENT, "expression is not supported inside comparison operator %s", string(key))
		}
		return nil
	})

	return valueMatcher, err
}
