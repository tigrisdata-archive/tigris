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
	"fmt"
	jsoniter "github.com/json-iterator/go"

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

	var decodeFilter = &structpb.Value{}
	if err := jsoniter.Unmarshal(reqFilter, decodeFilter); ulog.E(err) {
		return nil, err
	}

	structObj := decodeFilter.GetStructValue()
	if structObj == nil {
		return nil, status.Errorf(codes.InvalidArgument, "only object is allowed to be passed as filter '%s'", string(reqFilter))
	}

	var filters []Filter
	for key, reqF := range structObj.GetFields() {
		e, err := ParseFilter(key, reqF)
		if ulog.E(err) {
			return nil, err
		}

		f, ok := e.(Filter)
		if !ok {
			return nil, ulog.CE("not able to decode to filter %v", f)
		}

		filters = append(filters, f)
	}

	return filters, nil
}

func ParseFilter(name string, value *structpb.Value) (expression.Expr, error) {
	var err error
	var expr []expression.Expr

	switch name {
	case string(AndOP):
		if expr, err = expression.ParseList(value.GetListValue(), ParseFilter); err != nil {
			return nil, err
		}
		filters, err := convertExprListToFilters(expr)
		if err != nil {
			return nil, err
		}

		a, err := NewAndFilter(filters)
		if err != nil {
			return nil, err
		}
		return a, nil
	case string(OrOP):
		if expr, err = expression.ParseList(value.GetListValue(), ParseFilter); err != nil {
			return nil, err
		}
		filters, err := convertExprListToFilters(expr)
		if err != nil {
			return nil, err
		}

		o, err := NewOrFilter(filters)
		if err != nil {
			return nil, err
		}
		return o, nil
	default:
		return ParseSelector(name, value)
	}
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
func ParseSelector(key string, userValue *structpb.Value) (Filter, error) {
	switch ty := userValue.Kind.(type) {
	case *structpb.Value_NumberValue, *structpb.Value_StringValue, *structpb.Value_BoolValue:
		val := value.NewValue(userValue)
		if val == nil {
			return nil, fmt.Errorf("not able to create internal value %T", ty)
		}

		// just add equality by default if not specified by the user something like {"a": 10}
		return NewSelector(key, NewEqualityMatcher(val)), nil
	case *structpb.Value_StructValue:
		c, err := buildComparisonOperator(ty)
		if err != nil {
			return nil, err
		}
		return NewSelector(key, c), nil
	}
	return nil, nil
}

func buildComparisonOperator(value *structpb.Value_StructValue) (ValueMatcher, error) {
	if value == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty object")
	}
	if len(value.StructValue.GetFields()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty object")
	}

	for k, v := range value.StructValue.GetFields() {
		switch k {
		case EQ, GT:
			switch v.Kind.(type) {
			case *structpb.Value_NumberValue, *structpb.Value_StringValue, *structpb.Value_BoolValue, *structpb.Value_NullValue:
				// if value is simple
				return NewMatcher(k, v)
			case *structpb.Value_StructValue:
				// ToDo: support for nested expression object to extract the value
				return nil, status.Errorf(codes.InvalidArgument, "expression is not supported inside comparison operator %v", v)
			case *structpb.Value_ListValue:
				// ToDo: similar as above support for nested expression object to extract the value
				return nil, status.Errorf(codes.InvalidArgument, "list is not supported inside comparison operator %v", v)
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "expression is not supported inside comparison operator %s", k)
		}
	}

	return nil, status.Errorf(codes.InvalidArgument, "only object allowed inside filters is and/or or comparison operator")
}
