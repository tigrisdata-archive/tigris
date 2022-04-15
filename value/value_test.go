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

package value

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNewValue(t *testing.T) {
	type testStruct struct {
		A int
		B float64
		C string
		D []byte
	}

	ts := &testStruct{
		A: 1,
		B: 1.01,
		C: "foo",
		D: []byte(`"foo"`),
	}

	b, err := json.Marshal(ts)
	require.NoError(t, err)

	s := &structpb.Struct{}
	require.NoError(t, protojson.Unmarshal(b, s))

	v, err := NewValueUsingSchema(&schema.Field{FieldName: "A", DataType: schema.IntType}, s.Fields["A"])
	require.NoError(t, err)
	require.Equal(t, int64(1), v.AsInterface())

	v, err = NewValueUsingSchema(&schema.Field{FieldName: "B", DataType: schema.DoubleType}, s.Fields["B"])
	require.NoError(t, err)
	require.Equal(t, float64(1.01), v.AsInterface())

	v, err = NewValueUsingSchema(&schema.Field{FieldName: "C", DataType: schema.StringType}, s.Fields["C"])
	require.NoError(t, err)
	require.Equal(t, "foo", v.AsInterface())
}

func TestNewValueFromSchema(t *testing.T) {
	type testStruct struct {
		A int
		B float64
		C string
		D []byte
		E bool
	}

	ts := &testStruct{
		A: 1,
		B: 1.01,
		C: "foo",
		D: []byte(`"foo"`),
		E: true,
	}

	b, err := json.Marshal(ts)
	require.NoError(t, err)

	s := &structpb.Struct{}
	require.NoError(t, protojson.Unmarshal(b, s))
	cases := []struct {
		field    *schema.Field
		value    Value
		expError error
	}{
		{
			&schema.Field{
				FieldName: "A",
				DataType:  schema.IntType,
			},
			NewIntValue(1),
			nil,
		}, {
			&schema.Field{
				FieldName: "B",
				DataType:  schema.IntType,
			},
			nil,
			status.Errorf(codes.InvalidArgument, "permissible type for '%s' is integer only", "B"),
		}, {
			&schema.Field{
				FieldName: "B",
				DataType:  schema.DoubleType,
			},
			NewDoubleValue(1.01),
			nil,
		}, {
			&schema.Field{
				FieldName: "C",
				DataType:  schema.StringType,
			},
			NewStringValue("foo"),
			nil,
		}, {
			// we don't decode byte type
			&schema.Field{
				FieldName: "D",
				DataType:  schema.ByteType,
			},
			NewBytesValue([]byte(`ImZvbyI=`)),
			nil,
		}, {
			&schema.Field{
				FieldName: "E",
				DataType:  schema.BoolType,
			},
			NewBoolValue(true),
			nil,
		},
	}
	for _, c := range cases {
		v, err := NewValueFromStruct(c.field, s)
		require.Equal(t, c.expError, err)
		require.Equal(t, c.value, v)
	}
}

func TestIsIntegral(t *testing.T) {
	require.True(t, isIntegral(1))
	require.False(t, isIntegral(1.01))
	require.False(t, isIntegral(math.NaN()))
}

func TestValue(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		i := IntValue(5)

		v, err := NewValueUsingSchema(&schema.Field{DataType: schema.IntType}, structpb.NewNumberValue(5))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.IntType}, structpb.NewNumberValue(7))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.IntType}, structpb.NewNumberValue(0))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.StringType}, structpb.NewStringValue("5"))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("string", func(t *testing.T) {
		i := StringValue("abc")

		v, err := NewValueUsingSchema(&schema.Field{DataType: schema.StringType}, structpb.NewStringValue("abc"))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.StringType}, structpb.NewStringValue("acc"))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.StringType}, structpb.NewStringValue("abb"))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.IntType}, structpb.NewNumberValue(5))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("bool", func(t *testing.T) {
		i := BoolValue(false)

		v, err := NewValueUsingSchema(&schema.Field{DataType: schema.BoolType}, structpb.NewBoolValue(false))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.BoolType}, structpb.NewBoolValue(true))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		i = BoolValue(true)
		v, err = NewValueUsingSchema(&schema.Field{DataType: schema.BoolType}, structpb.NewBoolValue(false))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}
