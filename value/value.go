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
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"google.golang.org/grpc/codes"
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
	Comparable

	// AsInterface to return the value as interface
	AsInterface() interface{}
}

// NewValue returns the value of the field from the raw json value. It uses schema to get the type of the field.
func NewValue(fieldType schema.FieldType, value []byte) (Value, error) {
	switch fieldType {
	case schema.BoolType:
		b, err := strconv.ParseBool(string(value))
		if err != nil {
			return nil, api.Error(codes.InvalidArgument, errors.Wrap(err, "unsupported value type ").Error())
		}
		return NewBoolValue(b), nil
	case schema.DoubleType:
		val, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return nil, api.Error(codes.InvalidArgument, errors.Wrap(err, "unsupported value type ").Error())
		}
		return NewDoubleValue(val), nil
	case schema.Int32Type, schema.Int64Type:
		val, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, api.Error(codes.InvalidArgument, errors.Wrap(err, "unsupported value type ").Error())
		}

		return NewIntValue(int64(val)), nil
	case schema.StringType, schema.UUIDType, schema.DateTimeType:
		return NewStringValue(string(value)), nil
	case schema.ByteType:
		if decoded, err := base64.StdEncoding.DecodeString(string(value)); err == nil {
			// when we match the value or build the key we first decode the base64 data
			return NewBytesValue(decoded), nil
		} else {
			return NewBytesValue(value), nil
		}
	}

	return nil, api.Errorf(codes.InvalidArgument, "unsupported value type")
}

func isIntegral(val float64) bool {
	return val == float64(int(val))
}

type IntValue int64

func NewIntValue(v int64) *IntValue {
	i := IntValue(v)
	return &i
}

func (i *IntValue) CompareTo(v Value) (int, error) {
	if v == nil {
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
	} else {
		return 1, nil
	}
}

func (i *IntValue) AsInterface() interface{} {
	return int64(*i)
}

func (i *IntValue) String() string {
	if i == nil {
		return ""
	}

	return fmt.Sprintf("%d", *i)
}

type DoubleValue float64

func NewDoubleValue(v float64) *DoubleValue {
	i := DoubleValue(v)
	return &i
}

func (d *DoubleValue) CompareTo(v Value) (int, error) {
	if v == nil {
		return 1, nil
	}

	converted, ok := v.(*DoubleValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if *d == *converted {
		return 0, nil
	} else if *d < *converted {
		return -1, nil
	} else {
		return 1, nil
	}
}

func (d *DoubleValue) AsInterface() interface{} {
	return float64(*d)
}

func (d *DoubleValue) String() string {
	if d == nil {
		return ""
	}

	return fmt.Sprintf("%f", *d)
}

type StringValue string

func NewStringValue(v string) *StringValue {
	i := StringValue(v)
	return &i
}

func (s *StringValue) CompareTo(v Value) (int, error) {
	if v == nil {
		return 1, nil
	}

	converted, ok := v.(*StringValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if *s == *converted {
		return 0, nil
	} else if *s < *converted {
		return -1, nil
	} else {
		return 1, nil
	}
}

func (s *StringValue) AsInterface() interface{} {
	return string(*s)
}

func (s *StringValue) String() string {
	if s == nil {
		return ""
	}

	return string(*s)
}

type BytesValue string

func NewBytesValue(v []byte) *BytesValue {
	i := BytesValue(v)
	return &i
}

func (s *BytesValue) CompareTo(v Value) (int, error) {
	if v == nil {
		return 1, nil
	}

	converted, ok := v.(*BytesValue)
	if !ok {
		return -2, fmt.Errorf("wrong type compared ")
	}

	if *s == *converted {
		return 0, nil
	} else if *s < *converted {
		return -1, nil
	} else {
		return 1, nil
	}
}

func (s *BytesValue) AsInterface() interface{} {
	return []byte(*s)
}

func (s *BytesValue) String() string {
	if s == nil {
		return ""
	}

	return string(*s)
}

type BoolValue bool

func NewBoolValue(v bool) *BoolValue {
	i := BoolValue(v)
	return &i
}

func (b *BoolValue) CompareTo(v Value) (int, error) {
	if v == nil {
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

func (b *BoolValue) AsInterface() interface{} {
	return bool(*b)
}

func (b *BoolValue) String() string {
	if b == nil {
		return ""
	}

	return fmt.Sprintf("%v", *b)
}
