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
	"github.com/tigrisdata/tigris/errors"
	"github.com/ugorji/go/codec"
)

// NewStreamData returns a stream data for anything that we need to store in streams.
func NewStreamData(md []byte, data []byte) *StreamData {
	return &StreamData{
		Md:        md,
		RawData:   data,
		CreatedAt: NewTimestamp(),
	}
}

func EncodeStreamData(event *StreamData) ([]byte, error) {
	return encodeInternal(event, StreamDataType)
}

func DecodeStreamData(b []byte) (*StreamData, error) {
	if len(b) == 0 {
		return nil, errors.Internal("unable to decode event data is empty")
	}
	dataType := DataType(b[0])
	dec := codec.NewDecoderBytes(b[1:], &bh)

	if dataType == StreamDataType {
		var v *StreamData
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}

	return nil, errors.Internal("unable to decode '%v'", dataType)
}

func NewCacheData(data []byte) *CacheData {
	return &CacheData{
		CreatedAt: NewTimestamp(),
		RawData:   data,
	}
}

func EncodeCacheData(data *CacheData) ([]byte, error) {
	return encodeInternal(data, CacheDataType)
}

func DecodeCacheData(b []byte) (*CacheData, error) {
	if len(b) == 0 {
		return nil, errors.Internal("unable to decode event data is empty")
	}
	dataType := DataType(b[0])
	dec := codec.NewDecoderBytes(b[1:], &bh)

	if dataType == CacheDataType {
		var v *CacheData
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}

	return nil, errors.Internal("unable to decode '%v'", dataType)
}
