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

package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
)

func TestEncode_Decode(t *testing.T) {
	t.Run("table_data", func(t *testing.T) {
		d := NewTableData([]byte(`{"a": 1, "b": "foo"}`))
		encoded, err := Encode(d)
		require.NoError(t, err)
		require.NotNil(t, encoded)

		data, err := Decode(encoded)
		require.NoError(t, err)
		require.Equal(t, d, data)
		require.Equal(t, 20, data.Size())
	})
	t.Run("not_implemented", func(t *testing.T) {
		data, err := Decode([]byte(`{"a": 1, "b": "foo"}`))
		require.Equal(t, errors.Internal("unable to decode '123'"), err)
		require.Nil(t, data)
	})
}

func Benchmark_MsgPack(b *testing.B) {
	v := &TableData{
		RawData: []byte(`"K1": "vK1", "K2": 1, "D1": "vD1", "random", "this is a long string, with many characters"}`),
	}
	for n := 0; n < b.N; n++ {
		enc, err := Encode(v)
		require.NoError(b, err)
		require.NotNil(b, enc)
	}
}
