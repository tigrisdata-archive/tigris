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
	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/query/expression"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"github.com/tigrisdata/tigrisdb/value"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
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
	Matches(doc *structpb.Struct) bool
}

func Build(reqFilter []byte) ([]Filter, error) {
	if len(reqFilter) == 0 {
		return nil, nil
	}

	var seen = make(map[string]struct{})
	var filters []Filter
	var err error
	err = jsonparser.ObjectEach(reqFilter, func(k []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		var filter Filter
		switch string(k) {
		case string(AndOP):
			filter, err = UnmarshalAnd(v)
		case string(OrOP):
			filter, err = UnmarshalOr(v)
		default:
			filter, err = ParseSelector(k, v, dataType)
		}
		if err != nil {
			return err
		}
		if _, ok := seen[string(k)]; ok {
			return api.Errorf(codes.InvalidArgument, "duplicate filter '%s'", string(k))
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

func UnmarshalFilter(input jsoniter.RawMessage) (expression.Expr, error) {
	var err error
	var filter Filter
	err = jsonparser.ObjectEach(input, func(k []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch string(k) {
		case string(AndOP):
			filter, err = UnmarshalAnd(v)
		case string(OrOP):
			filter, err = UnmarshalOr(v)
		default:
			filter, err = ParseSelector(k, v, dataType)
		}
		return nil
	})

	return filter, err
}

func UnmarshalAnd(input jsoniter.RawMessage) (Filter, error) {
	expr, err := expression.UnmarshalArray(input, UnmarshalFilter)
	if err != nil {
		return nil, err
	}
	andFilters, err := convertExprListToFilters(expr)
	if err != nil {
		return nil, err
	}

	return NewAndFilter(andFilters)
}

func UnmarshalOr(input jsoniter.RawMessage) (Filter, error) {
	expr, err := expression.UnmarshalArray(input, UnmarshalFilter)
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
func ParseSelector(k []byte, v []byte, dataType jsonparser.ValueType) (Filter, error) {
	switch dataType {
	case jsonparser.Boolean, jsonparser.Number, jsonparser.String:
		val, err := value.NewValueFromByte(v, dataType)
		if err != nil {
			return nil, err
		}

		return NewSelector(string(k), NewEqualityMatcher(val)), nil
	case jsonparser.Object:
		valueMatcher, err := buildComparisonOperator(v)
		if err != nil {
			return nil, err
		}

		return NewSelector(string(k), valueMatcher), nil
	default:
		return nil, api.Errorf(codes.InvalidArgument, "unable to parse the comparison operator")
	}
}

func buildComparisonOperator(input jsoniter.RawMessage) (ValueMatcher, error) {
	if len(input) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty object")
	}

	var valueMatcher ValueMatcher
	var err error
	err = jsonparser.ObjectEach(input, func(key []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch string(key) {
		case EQ, GT:
			switch dataType {
			case jsonparser.Boolean, jsonparser.Number, jsonparser.String, jsonparser.Null:
				var val value.Value
				val, err = value.NewValueFromByte(v, dataType)
				if err != nil {
					return err
				}

				valueMatcher, err = NewMatcher(string(key), val)
				return err
			}
		default:
			return status.Errorf(codes.InvalidArgument, "expression is not supported inside comparison operator %s", string(key))
		}
		return nil
	})

	return valueMatcher, err
}
