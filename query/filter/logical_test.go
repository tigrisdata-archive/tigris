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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
)

func TestLogicalToSearch(t *testing.T) {
	factory := Factory{
		fields: []*schema.QueryableField{
			schema.NewQueryableField("f1", schema.Int64Type),
			schema.NewQueryableField("f2", schema.Int64Type),
			schema.NewQueryableField("f3", schema.Int64Type),
			schema.NewQueryableField("f4", schema.Int64Type),
			schema.NewQueryableField("f5", schema.Int64Type),
			schema.NewQueryableField("f6", schema.Int64Type),
			schema.NewQueryableField("a", schema.Int64Type),
			schema.NewQueryableField("b", schema.Int64Type),
			schema.NewQueryableField("c", schema.Int64Type),
			schema.NewQueryableField("d", schema.Int64Type),
			schema.NewQueryableField("e", schema.Int64Type),
			schema.NewQueryableField("f", schema.Int64Type),
		},
	}

	js := []byte(`{"f1": 10}`)
	testLogicalSearch(t, js, factory, []string{"f1:=10"})

	// f1=10&&f2=10
	js = []byte(`{"f1": 10, "f2": 10}`)
	testLogicalSearch(t, js, factory, []string{"f1:=10&&f2:=10"})

	js = []byte(`{"$or": [{"a":5}, {"b": 6}]}`)
	testLogicalSearch(t, js, factory, []string{"a:=5", "b:=6"})

	js = []byte(`{"$and": [{"a":5}, {"b": 6}]}`)
	testLogicalSearch(t, js, factory, []string{"a:=5&&b:=6"})

	// f2=5&&f2=6, f1=20
	js = []byte(`{"$or": [{"f1": 20}, {"$and": [{"f2":5}, {"f3": 6}]}]}`)
	testLogicalSearch(t, js, factory, []string{"f1:=20", "f2:=5&&f3:=6"})

	js = []byte(`{"$and": [{"a":5}, {"b": 6}, {"$or": [{"f1":5}, {"f2": 6}]}]}`)
	testLogicalSearch(t, js, factory, []string{"a:=5&&b:=6&&f1:=5", "a:=5&&b:=6&&f2:=6"})

	js = []byte(`{"$or": [{"a":5}, {"b": 6}, {"$and": [{"f1":5}, {"f2": 6}]}]}`)
	testLogicalSearch(t, js, factory, []string{"a:=5", "b:=6", "f1:=5&&f2:=6"})

	// two OR combinations "a=20&&b=5&&e=5&&f=6", "a=20&&c=6&&e=5&&f=6"
	js = []byte(`{"$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
	testLogicalSearch(t, js, factory, []string{"a:=20&&b:=5&&e:=5&&f:=6", "a:=20&&c:=6&&e:=5&&f:=6"})

	// Flattening will result in 4 OR combinations
	js = []byte(`{"f1": 10, "f2": 10, "$or": [{"f3": 20}, {"$and": [{"f4":5}, {"f5": 6}]}], "$and": [{"a": 20}, {"$or": [{"b":5}, {"c": 6}]}, {"$and": [{"e":5}, {"f": 6}]}]}`)
	testLogicalSearch(t, js, factory, []string{
		"f1:=10&&f2:=10&&f3:=20&&a:=20&&b:=5&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f3:=20&&a:=20&&c:=6&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f4:=5&&f5:=6&&a:=20&&b:=5&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f4:=5&&f5:=6&&a:=20&&c:=6&&e:=5&&f:=6",
	})
}

func testLogicalSearch(t *testing.T, js []byte, factory Factory, expConverted []string) {
	wrapped, err := factory.WrappedFilter(js)
	require.NoError(t, err)
	toSearch := wrapped.Filter.ToSearchFilter()
	require.Equal(t, expConverted, toSearch)
}
