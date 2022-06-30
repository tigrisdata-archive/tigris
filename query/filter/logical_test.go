package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/schema"
)

func TestLogicalToSearch(t *testing.T) {
	var factory = Factory{
		fields: []*schema.Field{
			{FieldName: "f1", DataType: schema.Int64Type},
			{FieldName: "f2", DataType: schema.Int64Type},
			{FieldName: "f3", DataType: schema.Int64Type},
			{FieldName: "f4", DataType: schema.Int64Type},
			{FieldName: "f5", DataType: schema.Int64Type},
			{FieldName: "f6", DataType: schema.Int64Type},
			{FieldName: "a", DataType: schema.Int64Type},
			{FieldName: "b", DataType: schema.Int64Type},
			{FieldName: "c", DataType: schema.Int64Type},
			{FieldName: "d", DataType: schema.Int64Type},
			{FieldName: "e", DataType: schema.Int64Type},
			{FieldName: "f", DataType: schema.Int64Type},
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
	testLogicalSearch(t, js, factory, []string{"f1:=10&&f2:=10&&f3:=20&&a:=20&&b:=5&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f3:=20&&a:=20&&c:=6&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f4:=5&&f5:=6&&a:=20&&b:=5&&e:=5&&f:=6",
		"f1:=10&&f2:=10&&f4:=5&&f5:=6&&a:=20&&c:=6&&e:=5&&f:=6"})
}

func testLogicalSearch(t *testing.T, js []byte, factory Factory, expConverted []string) {
	wrapped, err := factory.WrappedFilter(js)
	require.NoError(t, err)
	toSearch := wrapped.Filter.ToSearchFilter()
	require.Equal(t, expConverted, toSearch)
}
