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

package expression

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/value"
)

// Expr can be any operator, filter, field literal, etc. It is useful for parsing complex grammar, it can be nested.
type Expr interface{}

func Unmarshal(input jsoniter.RawMessage, objCb func(jsoniter.RawMessage) (Expr, error)) (Expr, error) {
	iter := jsoniter.ParseBytes(jsoniter.ConfigCompatibleWithStandardLibrary, input)
	next := iter.WhatIsNext()
	if next == jsoniter.InvalidValue {
		return nil, fmt.Errorf("invalid JSON '%s'", string(input))
	}

	switch next {
	case jsoniter.StringValue:
		return value.NewStringValue(iter.ReadString(), nil), nil
	case jsoniter.NumberValue:
		number := iter.ReadNumber()
		if i, err := number.Int64(); err == nil {
			return value.NewIntValue(i), nil
		}
		if i, err := number.Float64(); err == nil {
			return value.NewDoubleUsingFloat(i), nil
		}
		return nil, fmt.Errorf("not able to decode number")
	case jsoniter.BoolValue:
		return value.NewBoolValue(iter.ReadBool()), nil
	case jsoniter.ArrayValue:
		return UnmarshalArray(input, objCb)
	case jsoniter.ObjectValue:
		return objCb(input)
	case jsoniter.NilValue:
		return nil, errors.InvalidArgument("null is not a valid expression")
	}

	return nil, errors.InvalidArgument("not a valid expression")
}

func UnmarshalArray(input jsoniter.RawMessage, objCb func(jsoniter.RawMessage) (Expr, error)) ([]Expr, error) {
	var array []jsoniter.RawMessage

	var err error
	var expr []Expr
	err = jsoniter.Unmarshal(input, &array)

	for _, a := range array {
		var e Expr
		e, err = Unmarshal(a, objCb)
		if err != nil {
			return nil, err
		}

		if e == nil {
			return nil, errors.InvalidArgument("empty object detected")
		}

		expr = append(expr, e)
	}

	return expr, err
}
