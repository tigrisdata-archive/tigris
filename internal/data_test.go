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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"google.golang.org/protobuf/proto"
)

func TestEncode_Decode(t *testing.T) {
	t.Run("table_data", func(t *testing.T) {
		d := NewTableData([]byte(`{"a": 1, "b": "foo"}`))
		encoded, err := Encode(d)
		require.NoError(t, err)
		require.NotNil(t, encoded)

		data, err := Decode(encoded)
		require.NoError(t, err)
		require.True(t, proto.Equal(d, data))
	})
	t.Run("not_implemented", func(t *testing.T) {
		data, err := Decode([]byte(`{"a": 1, "b": "foo"}`))
		require.Equal(t, errors.Internal("unable to decode '123'"), err)
		require.Nil(t, data)
	})

	t.Run("empty", func(t *testing.T) {
		v := &TableData{
			RawData: []byte(`{"a": 1, "b": "foo"}`),
		}

		encoded, err := Encode(v)
		require.NoError(t, err)
		require.NotNil(t, encoded)

		data, err := Decode(encoded)
		require.NoError(t, err)
		require.True(t, proto.Equal(v, data))
	})

	t.Run("decode_binc", func(t *testing.T) {
		d := NewTableData([]byte(`{"a": 1, "b": "foo"}`))
		encoded, err := encodeInternal(d, TableDataType)
		require.NoError(t, err)
		require.NotNil(t, encoded)

		data, err := Decode(encoded)
		require.NoError(t, err)
		require.True(t, proto.Equal(d, data))
	})

	t.Run("decode_proto", func(t *testing.T) {
		d := NewTableData([]byte(`{"a": 1, "b": "foo"}`))
		encoded, err := encodeInternalProto(d)
		require.NoError(t, err)
		require.NotNil(t, encoded)

		data, err := Decode(encoded)
		require.NoError(t, err)
		require.True(t, proto.Equal(d, data))
	})

	t.Run("default_is_proto", func(t *testing.T) {
		v := &TableData{
			RawData: []byte(`{"a": 1, "b": "foo"}`),
		}

		encoded, err := Encode(v)
		require.NoError(t, err)
		require.NotNil(t, encoded)
		require.Equal(t, TableDataProtoType, encoded[0])
	})

	t.Run("stable_constants", func(t *testing.T) {
		require.Equal(t, byte(1), TableDataType)
		require.Equal(t, byte(2), CacheDataType)
		require.Equal(t, byte(3), StreamDataType)
		require.Equal(t, byte(4), TableDataProtoType)
	})
}

func Benchmark_Encode(b *testing.B) {
	tm1 := NewTimestamp()
	tm2 := NewTimestamp()

	bb := int32(7)
	v := &TableData{
		Ver:         23045,
		Encoding:    3789,
		CreatedAt:   tm1,
		UpdatedAt:   tm2,
		RawData:     []byte("[s" + strings.Repeat("a", 1024*1024) + "e]"),
		TotalChunks: &bb,
	}

	b.Run("ugorji_binc", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			enc, err := encodeInternal(v, TableDataType)
			if err != nil {
				panic(err)
			}
			_ = enc
		}
	})

	b.Run("proto", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			enc, err := encodeInternalProto(v)
			if err != nil {
				panic(err)
			}
			_ = enc
		}
	})
	/*
		enc, err := encodeInternalProto(v)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "Size: %v\n", len(enc))
	*/
}

func Benchmark_Decode(b *testing.B) {
	tm1 := NewTimestamp()
	tm2 := NewTimestamp()

	bb := int32(7)
	v := &TableData{
		Ver:         23045,
		Encoding:    3789,
		CreatedAt:   tm1,
		UpdatedAt:   tm2,
		RawData:     []byte("[s" + strings.Repeat("a", 1024*1024) + "e]"),
		TotalChunks: &bb,
	}
	enc, err := encodeInternal(v, TableDataType)
	if err != nil {
		panic(err)
	}
	b.Run("ugorji_binc", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			td, err := decodeInternal(enc[1:])
			if err != nil {
				panic(err)
			}
			_ = td
		}
	})
	//	td, _ := decodeInternal(enc[1:])
	//	fmt.Fprintf(os.Stderr, "td: %+v\n", td)

	enc, err = encodeInternalProto(v)
	if err != nil {
		panic(err)
	}
	b.Run("proto", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			enc, err := decodeInternalProto(enc[1:])
			if err != nil {
				panic(err)
			}
			_ = enc
		}
	})

	/*
		td, err := decodeInternalProto(enc[1:])
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "td: %+v\n", td)
	*/
}
