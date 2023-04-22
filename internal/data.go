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
	"encoding/binary"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	UserTableKeyPrefix      = []byte("data")
	SecondaryTableKeyPrefix = []byte("idx")
	SearchTableKeyPrefix    = []byte("sea")
	PartitionKeyPrefix      = []byte("part")
	CacheKeyPrefix          = "cache"
)

var bh codec.BincHandle

// Note: Do not change the order. Order is important because encoder is adding the type as the first byte. Check the
// Encode/Decode method to see how it is getting used.
const (
	_ byte = iota
	TableDataType
	CacheDataType
	StreamDataType
	TableDataProtoType
)

type UserDataEncType int8

const (
	MsgpackEncoding UserDataEncType = 1
	JsonEncoding    UserDataEncType = 2
)

var EmptyData = &TableData{}

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

func (x *TableData) CloneWithAttributesOnly(newRawData []byte) *TableData {
	return &TableData{
		Ver:         x.Ver,
		Encoding:    x.Encoding,
		CreatedAt:   x.CreatedAt,
		UpdatedAt:   x.UpdatedAt,
		TotalChunks: x.TotalChunks,
		Compression: x.Compression,
		RawData:     newRawData,
		RawSize:     x.RawSize,
	}
}

// Size of the payload field.
func (x *TableData) Size() int32 {
	return int32(len(x.RawData))
}

func (x *TableData) IsChunkedData() bool {
	return x.TotalChunks != nil && *x.TotalChunks > 1
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

func (x *TableData) TimeStampsToJSON() ([]byte, error) {
	data := map[string]jsoniter.RawMessage{
		"_tigris_created_at": nil,
		"_tigris_updated_at": nil,
	}

	if x.CreatedAt != nil {
		data["_tigris_created_at"] = jsoniter.RawMessage(x.CreatedAt.ToRFC3339())
	}

	if x.UpdatedAt != nil {
		data["_tigris_updated_at"] = jsoniter.RawMessage(x.CreatedAt.ToRFC3339())
	}

	return jsoniter.Marshal(data)
}

// ActualUserPayloadSize returns size of the user data. This is used by splitter to split the value if it is
// greater than the maximum allowed by the underlying storage.
func (x *TableData) ActualUserPayloadSize() int32 {
	return int32(len(x.RawData))
}

func (x *TableData) SetTotalChunks(chunkSize int32) {
	totalChunks := (x.ActualUserPayloadSize() + chunkSize - 1) / chunkSize
	x.TotalChunks = &totalChunks
}

// Encode is used to encode data to the raw bytes which is used to store in storage as value. The first byte is storing
// the type corresponding to this Data. This is important and used by the decoder later to decode back.
func Encode(data *TableData) ([]byte, error) {
	return encodeInternalProto(data)
}

func encodeInternal(data any, dataType byte) ([]byte, error) {
	var buf bytes.Buffer

	// this is added so that we can evolve the DataTypes and have more dataTypes in future
	if err := buf.WriteByte(dataType); err != nil {
		return nil, err
	}

	enc := codec.NewEncoder(&buf, &bh)
	if err := enc.Encode(data); ulog.E(err) {
		return nil, err
	}

	return buf.Bytes(), nil
}

func encodeInternalProto(data *TableData) ([]byte, error) {
	var buf bytes.Buffer

	// one byte of data type and two bytes is proto length placeholder
	// which is used to calculate payload of offset in the buffer.
	if _, err := buf.Write([]byte{TableDataProtoType, 0x00, 0x00}); err != nil {
		return nil, err
	}

	// remember and clear payload field in the input
	payload := data.RawData
	data.RawData = nil

	md, err := proto.Marshal(data)
	if err != nil {
		return nil, err
	}

	data.RawData = payload // restore the payload

	if _, err = buf.Write(md); err != nil {
		return nil, err
	}

	if _, err = buf.Write(payload); err != nil {
		return nil, err
	}

	b := buf.Bytes()

	// fill the marshalled proto message size
	binary.BigEndian.PutUint16(b[1:], uint16(len(md)))

	return b, nil
}

// Decode is used to decode the raw bytes to TableData. The raw bytes are returned from the storage and the kvStore is
// calling Decode to convert these raw bytes back to TableData.
func Decode(b []byte) (*TableData, error) {
	if len(b) == 0 {
		return nil, errors.Internal("unable to decode table data is empty")
	}

	switch b[0] {
	case TableDataType:
		return decodeInternal(b[1:])
	case TableDataProtoType:
		return decodeInternalProto(b[1:])
	}

	return nil, errors.Internal("unable to decode '%v'", b[0])
}

func decodeInternal(encoded []byte) (*TableData, error) {
	dec := codec.NewDecoderBytes(encoded, &bh)

	var v *TableData

	if err := dec.Decode(&v); err != nil {
		return nil, err
	}

	return v, nil
}

func decodeInternalProto(encoded []byte) (*TableData, error) {
	length := binary.BigEndian.Uint16(encoded) // get the length of the proto message

	var v TableData

	if err := proto.Unmarshal(encoded[2:length+2], &v); err != nil {
		return nil, err
	}

	v.RawData = encoded[length+2:] // put a pointer to the payload in the buffer

	return &v, nil
}
