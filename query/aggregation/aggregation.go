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

package aggregation

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/query/expression"
)

// Aggregation operators either can pass a single expression or an array of expression. The below is an example of
// how a caller can pass aggregation,
//
// { $aggregation: <expression> }
//
//	OR
//
// { $aggregation: [ <expression1>, <expression2> ... ] }
//
// As an example, for sum across two fields the grammar would be,
//
//	{
//		"$sum": {
//			"$multiply": ["$price", "$quantity"]
//		}
//	}
//
// More examples,
//
//	{
//		"$sum": "$qty"
//	}
//
// { "$sum": [ "$final", "$midterm" ] }}.
type Aggregation interface {
	Apply(document jsoniter.RawMessage)
}

// Unmarshal to unmarshal an aggregation object.
func Unmarshal(input jsoniter.RawMessage) (expression.Expr, error) {
	return expression.Unmarshal(input, UnmarshalAggObject)
}

// UnmarshalAggObject unmarshal the input to the aggregation. Note the return after the first check, this is mainly
// because an aggregation object can have nested objects but top level it will be one expression.
func UnmarshalAggObject(input jsoniter.RawMessage) (expression.Expr, error) {
	var err error
	var mp map[string]jsoniter.RawMessage
	if err = jsoniter.Unmarshal(input, &mp); err != nil {
		return nil, err
	}

	for key := range mp {
		switch key {
		case add, multiply:
			var f ArithmeticFactory
			if err = jsoniter.Unmarshal(input, &f); err != nil {
				return nil, err
			}
			return f.Get(), nil
		case avg, min, max, sum:
			var f AccumulatorFactory
			if err = jsoniter.Unmarshal(input, &f); err != nil {
				return nil, err
			}
			return f.Get(), nil
		default:
			return nil, fmt.Errorf("unsupported aggregation found '%s'", key)
		}
	}

	return nil, fmt.Errorf("unsupported aggregation found")
}
