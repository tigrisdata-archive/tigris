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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
)

func TestLogicalToSearch(t *testing.T) {
	factory := Factory{
		fields: []*schema.QueryableField{
			schema.NewQueryableFieldsBuilder().NewQueryableField("f1", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f2", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f3", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f4", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f5", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f6", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("a", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("b", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("c", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("d", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("e", &schema.Field{DataType: schema.Int64Type}, nil),
			schema.NewQueryableFieldsBuilder().NewQueryableField("f", &schema.Field{DataType: schema.Int64Type}, nil),
		},
	}

	js := []byte(`{"f1": 10}`)
	testLogicalSearch(t, js, factory, "f1:=10")


	js = []byte(`{"f1": 10, "f2": 10}`)
	testLogicalSearch(t, js, factory, "f1:=10 && f2:=10")

	js = []byte(`{"$or": [{"a":5}, {"b": 6}]}`)
	testLogicalSearch(t, js, factory, "a:=5 || b:=6")

	js = []byte(`{"$and": [{"a":5}, {"b": 6}]}`)
	testLogicalSearch(t, js, factory, "a:=5 && b:=6")

	// f2=5&&f2=6, f1=20
	js = []byte(`{"$or": [{"f1": 20}, {"$and": [{"f2":5}, {"f3": 6}]}]}`)
	testLogicalSearch(t, js, factory, "f1:=20 || (f2:=5 && f3:=6)")

	js = []byte(`{"$and": [{"a":5}, {"b": 6}, {"$or": [{"f1":5}, {"f2": 6}]}]}`)
	testLogicalSearch(t, js, factory, "a:=5 && b:=6 && (f1:=5 || f2:=6)")

	js = []byte(`{"$or": [{"a":5}, {"b": 6}, {"$and": [{"f1":5}, {"f2": 6}]}]}`)
	testLogicalSearch(t, js, factory, "a:=5 || b:=6 || (f1:=5 && f2:=6)")

	// two OR combinations "a=20&&b=5&&e=5&&f=6", "a=20&&c=6&&e=5&&f=6"
	js = []byte(`{"$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
	testLogicalSearch(t, js, factory, "a:=20 && (b:=5 || c:=6) && (e:=5 && f:=6)")

	// Flattening will result in 4 OR combinations
	js = []byte(`{"f1": 10, "f2": 10, "$or": [{"f3": 20}, {"$and": [{"f4":5}, {"f5": 6}]}], "$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
	testLogicalSearch(t, js, factory, "f1:=10 && f2:=10 && (f3:=20 || (f4:=5 && f5:=6)) && (a:=20 && (b:=5 || c:=6) && (e:=5 && f:=6))")
}

func testLogicalSearch(t *testing.T, js []byte, factory Factory, expConverted string) {
	wrapped, err := factory.WrappedFilter(js)
	require.NoError(t, err)
	toSearch := wrapped.Filter.ToSearchFilter()
	require.Equal(t, expConverted, toSearch)
}
