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
	structpb "github.com/gogo/protobuf/types"
)

// Expr can be any operator, filter, field literal, etc. It is useful for parsing complex grammar, it can be nested.
type Expr interface{}

// ParseList is used to parse any expression that is list.
func ParseList(list *structpb.ListValue, cb func(value *structpb.Struct) (Expr, error)) ([]Expr, error) {
	var items []Expr
	for _, value := range list.Values {
		item, err := ParseExpr(value, cb)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

// ParseExpr is used to parse any expression. It expects a callback that is used to parse structs/maps.
func ParseExpr(value *structpb.Value, cb func(value *structpb.Struct) (Expr, error)) (Expr, error) {
	if listValue := value.GetListValue(); listValue != nil {
		items, err := ParseList(listValue, cb)
		if err != nil {
			return nil, err
		}

		return &items, nil
	}

	return cb(value.GetStructValue())
}
