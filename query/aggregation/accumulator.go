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

// supported accumulators
const (
	avg = "$avg"
	min = "$min"
	max = "$max"
	sum = "$sum"
)

// AccumulatorFactory to return the object of the accumulator type
type AccumulatorFactory struct {
	Avg *AccumulatorOp `json:"$avg,omitempty"`
	Min *AccumulatorOp `json:"$min,omitempty"`
	Max *AccumulatorOp `json:"$max,omitempty"`
	Sum *AccumulatorOp `json:"$sum,omitempty"`
}

func (a *AccumulatorFactory) Get() Aggregation {
	if a.Avg != nil {
		a.Avg.Type = avg
		return a.Avg
	}
	if a.Min != nil {
		a.Min.Type = min
		return a.Min
	}
	if a.Max != nil {
		a.Max.Type = max
		return a.Max
	}
	if a.Sum != nil {
		a.Sum.Type = sum
		return a.Sum
	}
	return nil
}

// AccumulatorOp is a type of aggregation that can also be use in the group by to group values into subsets.
type AccumulatorOp struct {
	Type string
	Agg  expression.Expr
}

func (a *AccumulatorOp) UnmarshalJSON(input []byte) error {
	expr, err := expression.Unmarshal(input, UnmarshalAggObject)
	if err != nil {
		return err
	}

	a.Agg = expr
	return nil
}

func (a *AccumulatorOp) Apply(document jsoniter.RawMessage) {}

func (a *AccumulatorOp) String() string {
	return fmt.Sprintf(`{"%s": %v}`, a.Type, a.Agg)
}
