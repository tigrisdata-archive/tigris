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

package value

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
)

const (
	// SmallestNonZeroNormalFloat32 is the smallest positive non-zero floating number. The go package version
	// has the denormalized form which is higher than this.
	SmallestNonZeroNormalFloat32 = 0x1p-126
)

type Comparable interface {
	// CompareTo returns a value indicating the relationship between the receiver and the parameter.
	//
	// Returns a negative integer, zero, or a positive integer as the receiver is less than, equal
	// to, or greater than the parameter i.e. v1.CompareTo(v2) returns -1 if v1 < v2
	CompareTo(v Value) (int, error)
}

// Value is our value object that implements comparable so that two values can be compared. This is used to build the
// keys(primary key or any other index key), or to build the selector filter.
// Note: if the field data type is byte/binary then the value object returned is base64 decoded. The reason is that
// JSON has encoded the byte array to base64 so to make sure we are using the user provided value in building the key
// and the filter we must first decode this field. This allows us later to perform prefix scans.
type Value interface {
	fmt.Stringer
	Comparable

	// AsInterface to return the value as interface
	AsInterface() any
	DataType() schema.FieldType
}

func NewValueUsingCollation(fieldType schema.FieldType, value []byte, collation *Collation) (Value, error) {
	if isNull(fieldType, value) {
		return NewNullValue(), nil
	}

	if fieldType == schema.StringType && len(value) > 0 {
		parsed, err := jsonparser.ParseString(value)
		if err != nil {
			return nil, err
		}
		return NewStringValue(parsed, collation), nil
	}

	return NewValue(fieldType, value)
}

// NewValue returns the value of the field from the raw json value. It uses schema to get the type of the field.
func NewValue(fieldType schema.FieldType, value []byte) (Value, error) {
	if isNull(fieldType, value) {
		return NewNullValue(), nil
	}

	switch fieldType {
	case schema.BoolType:
		b, err := strconv.ParseBool(string(value))
		if err != nil {
			return nil, errors.InvalidArgument(fmt.Errorf("unsupported value type: %w", err).Error())
		}
		return NewBoolValue(b), nil
	case schema.DoubleType:
		return NewDoubleValue(string(value))
	case schema.Int32Type, schema.Int64Type:
		val, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, errors.InvalidArgument(fmt.Errorf("unsupported value type: %w", err).Error())
		}

		return NewIntValue(val), nil
	case schema.StringType, schema.UUIDType:
		parsed, err := jsonparser.ParseString(value)
		if err != nil {
			return nil, err
		}
		return NewStringValue(parsed, nil), nil
	case schema.DateTimeType:
		return NewDateTimeValue(string(value)), nil
	case schema.ByteType:
		if decoded, err := base64.StdEncoding.DecodeString(string(value)); err == nil {
			// when we match the value or build the key we first decode the base64 data
			return NewBytesValue(decoded), nil
		}
		return NewBytesValue(value), nil
	case schema.ArrayType:
		var arr []any
		if err := jsoniter.Unmarshal(value, &arr); err != nil {
			return nil, err
		}
		return NewArrayValue(value, arr), nil
	}

	return nil, errors.InvalidArgument("unsupported value type")
}

func isIntegral(val float64) bool {
	return val == float64(int(val))
}

func isNull(fieldType schema.FieldType, value []byte) bool {
	if fieldType == schema.NullType || len(value) == 0 || string(value) == "null" {
		return true
	}

	return false
}

func isNullValue(v Value) bool {
	if v == nil || v.String() == "" {
		return true
	}

	return false
}

type ArrayValue struct {
	raw     []byte
	decoded []any
}

func NewArrayValue(v []byte, decoded []any) *ArrayValue {
	return &ArrayValue{
		raw:     v,
		decoded: decoded,
	}
}

func (a *ArrayValue) CompareTo(v Value) (int, error) {
	if v == nil {
		return 1, nil
	}

	converted, ok := v.(*ArrayValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}
	return bytes.Compare(a.raw, converted.raw), nil
}

func (a *ArrayValue) AsInterface() any {
	return a.decoded
}

func (*ArrayValue) DataType() schema.FieldType {
	return schema.ArrayType
}

func (a *ArrayValue) String() string {
	if a == nil {
		return ""
	}

	return fmt.Sprintf("%v", *a)
}

type IntValue int64

func NewIntValue(v int64) *IntValue {
	i := IntValue(v)
	return &i
}

func (i *IntValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*IntValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if *i == *converted {
		return 0, nil
	} else if *i < *converted {
		return -1, nil
	}

	return 1, nil
}

func (i *IntValue) AsInterface() any {
	return int64(*i)
}

func (*IntValue) DataType() schema.FieldType {
	return schema.Int64Type
}

func (i *IntValue) String() string {
	if i == nil {
		return ""
	}

	return fmt.Sprintf("%d", *i)
}

type DoubleValue struct {
	Double   float64
	asString string
	Float    *big.Float
}

func NewDoubleUsingFloat(v float64) *DoubleValue {
	return &DoubleValue{
		Double:   v,
		asString: strconv.FormatFloat(v, 'f', -1, 64),
		Float:    big.NewFloat(v),
	}
}

func NewDoubleValue(raw string) (*DoubleValue, error) {
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, errors.InvalidArgument(fmt.Errorf("unsupported value type: %w ", err).Error())
	}

	float, _, err := big.ParseFloat(raw, 0, 1000, big.ToNearestEven)
	if err != nil {
		return nil, errors.InvalidArgument(fmt.Errorf("unsupported value type: %w ", err).Error())
	}

	i := &DoubleValue{
		Double:   val,
		Float:    float,
		asString: strconv.FormatFloat(val, 'f', -1, 64),
	}
	return i, nil
}

func (d *DoubleValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*DoubleValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	return d.Float.Cmp(converted.Float), nil
}

func (d *DoubleValue) AsInterface() any {
	return d.Double
}

func (*DoubleValue) DataType() schema.FieldType {
	return schema.DoubleType
}

func (d *DoubleValue) String() string {
	if d == nil {
		return ""
	}

	return d.asString
}

type StringValue struct {
	Value     string
	Collation *Collation
}

func NewStringValue(v string, collation *Collation) *StringValue {
	if collation == nil {
		collation = NewCollation()
	}

	s := &StringValue{
		Value:     v,
		Collation: collation,
	}

	return s
}

func (s *StringValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*StringValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	return s.Collation.CompareString(s.Value, converted.Value), nil
}

func (s *StringValue) AsInterface() any {
	if s.Collation.IsCollationSortKey() {
		return s.Collation.GenerateSortKey(s.Value)
	}

	return s.Value
}

func (*StringValue) DataType() schema.FieldType {
	return schema.StringType
}

func (s *StringValue) String() string {
	if s == nil {
		return ""
	}

	return s.Value
}

type DateTimeValue struct {
	Value string
}

func NewDateTimeValue(v string) *DateTimeValue {
	return &DateTimeValue{
		Value: v,
	}
}

func (d *DateTimeValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*DateTimeValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if d.Value == converted.Value {
		return 0, nil
	} else if d.Value < converted.Value {
		return -1, nil
	}

	return 1, nil
}

func (d *DateTimeValue) AsInterface() any {
	return d.Value
}

func (*DateTimeValue) DataType() schema.FieldType {
	return schema.DateTimeType
}

func (d *DateTimeValue) String() string {
	if d == nil {
		return ""
	}

	return d.Value
}

type BytesValue []byte

func NewBytesValue(v []byte) *BytesValue {
	i := BytesValue(v)
	return &i
}

func (b *BytesValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*BytesValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	return bytes.Compare(*b, *converted), nil
}

func (b *BytesValue) AsInterface() any {
	return []byte(*b)
}

func (*BytesValue) DataType() schema.FieldType {
	return schema.ByteType
}

func (b *BytesValue) String() string {
	if b == nil {
		return ""
	}

	return string(*b)
}

type BoolValue bool

func NewBoolValue(v bool) *BoolValue {
	i := BoolValue(v)
	return &i
}

func (b *BoolValue) CompareTo(v Value) (int, error) {
	if isNullValue(v) {
		return 1, nil
	}

	converted, ok := v.(*BoolValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if !bool(*b) && bool(*converted) {
		return -1, nil
	}
	if bool(*b) && !bool(*converted) {
		return 1, nil
	}
	return 0, nil
}

func (b *BoolValue) AsInterface() any {
	return bool(*b)
}

func (*BoolValue) DataType() schema.FieldType {
	return schema.BoolType
}

func (b *BoolValue) String() string {
	if b == nil {
		return ""
	}

	return fmt.Sprintf("%v", *b)
}

type NullValue struct{}

func NewNullValue() *NullValue {
	return &NullValue{}
}

func (*NullValue) AsInterface() any {
	return nil
}

func (*NullValue) DataType() schema.FieldType {
	return schema.NullType
}

func (*NullValue) String() string {
	return ""
}

func (*NullValue) CompareTo(v Value) (int, error) {
	if v == nil {
		return 0, nil
	}

	if _, ok := v.(*NullValue); ok {
		return 0, nil
	}

	return -1, nil
}

type MaxValue struct{}

func NewMaxValue() *MaxValue {
	return &MaxValue{}
}

func (*MaxValue) AsInterface() any {
	return 0xFF
}

func (*MaxValue) DataType() schema.FieldType {
	return schema.MaxType
}

func (*MaxValue) String() string {
	return "$MAX_VALUE"
}

func (*MaxValue) CompareTo(v Value) (int, error) {
	if _, ok := v.(*MaxValue); ok {
		return 0, nil
	}

	return 1, nil
}

func MinOrderValue() Value {
	return NewNullValue()
}

func MaxOrderValue() Value {
	return NewMaxValue()
}
