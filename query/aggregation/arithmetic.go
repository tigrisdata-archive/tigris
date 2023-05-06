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

package aggregation

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/query/expression"
)

// supported arithmetic operators.
const (
	add      = "$add"
	multiply = "$multiply"
)

// ArithmeticFactory to return the object of the arithmeticOp type.
type ArithmeticFactory struct {
	Add      *ArithmeticOp `json:"$add,omitempty"`
	Multiply *ArithmeticOp `json:"$multiply,omitempty"`
}

func (a *ArithmeticFactory) Get() Aggregation {
	if a.Multiply != nil {
		a.Multiply.Type = multiply
		return a.Multiply
	}
	if a.Add != nil {
		a.Add.Type = add
		return a.Add
	}
	return nil
}

type ArithmeticOp struct {
	Type string
	Agg  expression.Expr
}

func (a *ArithmeticOp) UnmarshalJSON(input []byte) error {
	expr, err := expression.Unmarshal(input, UnmarshalAggObject)
	if err != nil {
		return err
	}

	a.Agg = expr
	return nil
}

func (*ArithmeticOp) Apply(_ jsoniter.RawMessage) {}

func (a *ArithmeticOp) String() string {
	return fmt.Sprintf(`{"%s": %v}`, a.Type, a.Agg)
}
