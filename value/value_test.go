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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"google.golang.org/grpc/codes"
)

func TestNewValue(t *testing.T) {
	cases := []struct {
		field     schema.FieldType
		jsonValue []byte
		value     Value
		expError  error
	}{
		{
			schema.Int64Type,
			[]byte(`12345678`),
			NewIntValue(12345678),
			nil,
		}, {
			schema.Int32Type,
			[]byte(`"1"`),
			nil,
			api.Errorf(codes.InvalidArgument, "unsupported value type : strconv.ParseInt: parsing \"\\\"1\\\"\": invalid syntax"),
		}, {
			schema.DoubleType,
			[]byte(`1.01`),
			NewDoubleValue(1.01),
			nil,
		}, {
			schema.StringType,
			[]byte(`foo`),
			NewStringValue("foo"),
			nil,
		}, {
			// we decode byte type because it was encoded to base64 by JSON encoding.
			schema.ByteType,
			[]byte(`ImZvbyI=`),
			NewBytesValue([]byte(`"foo"`)),
			nil,
		}, {
			schema.BoolType,
			[]byte(`true`),
			NewBoolValue(true),
			nil,
		},
	}
	for _, c := range cases {
		v, err := NewValue(c.field, c.jsonValue)
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

		v, err := NewValue(schema.Int64Type, []byte(`5`))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValue(schema.Int64Type, []byte(`7`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		v, err = NewValue(schema.Int64Type, []byte(`0`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)

		v, err = NewValue(schema.StringType, []byte(`"5"`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("string", func(t *testing.T) {
		i := StringValue("abc")

		v, err := NewValue(schema.StringType, []byte(`abc`))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValue(schema.StringType, []byte(`acc`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		v, err = NewValue(schema.StringType, []byte(`abb`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)

		v, err = NewValue(schema.Int64Type, []byte(`5`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("bool", func(t *testing.T) {
		i := BoolValue(false)

		v, err := NewValue(schema.BoolType, []byte(`false`))
		require.NoError(t, err)
		r, err := i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 0, r)

		v, err = NewValue(schema.BoolType, []byte(`true`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, -1, r)

		i = BoolValue(true)
		v, err = NewValue(schema.BoolType, []byte(`false`))
		require.NoError(t, err)
		r, err = i.CompareTo(v)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}
