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
	"bytes"
	"time"

	"github.com/tigrisdata/tigris/errors"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	UserTableKeyPrefix = []byte("data")
	PartitionKeyPrefix = []byte("part")
	CacheKeyPrefix     = "cache"
)

var bh codec.BincHandle

// DataType is to define the different data types for the data stored in the storage engine.
type DataType byte

// Note: Do not change the order. Order is important because encoder is adding the type as the first byte. Check the
// Encode/Decode method to see how it is getting used.
const (
	Unknown DataType = iota
	TableDataType
	CacheDataType
	StreamDataType
)

type UserDataEncType int8

const (
	MsgpackEncoding UserDataEncType = 1
	JsonEncoding    UserDataEncType = 2
)

// CreateNewTimestamp is a method used to construct timestamp from unixNano. In search backend we store internal
// timestamps as unixNano.
func CreateNewTimestamp(nano int64) *Timestamp {
	ts := time.Unix(0, nano).UTC()
	return &Timestamp{
		Seconds:     ts.Unix(),
		Nanoseconds: int64(ts.Nanosecond()),
	}
}

func NewTimestamp() *Timestamp {
	ts := time.Now().UTC()
	return &Timestamp{
		Seconds:     ts.Unix(),
		Nanoseconds: int64(ts.Nanosecond()),
	}
}

func (x *Timestamp) ToRFC3339() string {
	goTime := time.Unix(x.Seconds, x.Nanoseconds).UTC()
	return goTime.Format(time.RFC3339Nano)
}

func (x *Timestamp) GetProtoTS() *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: x.Seconds,
		Nanos:   int32(x.Nanoseconds),
	}
}

// UnixNano returns t as a Unix time, the number of nanoseconds elapsed since January 1, 1970 UTC.
func (x *Timestamp) UnixNano() int64 {
	return x.Seconds*int64(time.Second) + x.Nanoseconds
}

// NewTableData returns a table data type by setting the ts to the current value.
func NewTableData(data []byte) *TableData {
	return &TableData{
		CreatedAt: NewTimestamp(),
		RawData:   data,
	}
}

func NewTableDataWithTS(createdAt *Timestamp, updatedAt *Timestamp, data []byte) *TableData {
	return &TableData{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		RawData:   data,
	}
}

func NewTableDataWithVersion(data []byte, version int32) *TableData {
	return &TableData{
		CreatedAt: NewTimestamp(),
		RawData:   data,
		Ver:       version,
	}
}

func (x *TableData) SetVersion(ver int32) {
	x.Ver = ver
}

func (x *TableData) CreateToProtoTS() *timestamppb.Timestamp {
	if x.CreatedAt != nil {
		return x.CreatedAt.GetProtoTS()
	}
	return nil
}

func (x *TableData) UpdatedToProtoTS() *timestamppb.Timestamp {
	if x.UpdatedAt != nil {
		return x.UpdatedAt.GetProtoTS()
	}
	return nil
}

// Encode is used to encode data to the raw bytes which is used to store in storage as value. The first byte is storing
// the type corresponding to this Data. This is important and used by the decoder later to decode back.
func Encode(data *TableData) ([]byte, error) {
	return encodeInternal(data, TableDataType)
}

func encodeInternal(data interface{}, typ DataType) ([]byte, error) {
	var buf bytes.Buffer
	// this is added so that we can evolve the DataTypes and have more dataTypes in future
	err := buf.WriteByte(byte(typ))
	if err != nil {
		return nil, err
	}
	enc := codec.NewEncoder(&buf, &bh)
	if err := enc.Encode(data); ulog.E(err) {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode is used to decode the raw bytes to TableData. The raw bytes are returned from the storage and the kvStore is
// calling Decode to convert these raw bytes back to TableData.
func Decode(b []byte) (*TableData, error) {
	if len(b) == 0 {
		return nil, errors.Internal("unable to decode table data is empty")
	}
	dataType := DataType(b[0])
	return decodeInternal(dataType, b[1:])
}

func decodeInternal(dataType DataType, encoded []byte) (*TableData, error) {
	dec := codec.NewDecoderBytes(encoded, &bh)

	if dataType == TableDataType {
		var v *TableData
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}

	return nil, errors.Internal("unable to decode '%v'", dataType)
}
