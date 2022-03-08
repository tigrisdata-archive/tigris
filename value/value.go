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

	"github.com/buger/jsonparser"

	"github.com/tigrisdata/tigrisdb/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type Comparable interface {
	// CompareTo returns a value indicating the relationship between the receiver and the parameter.
	//
	// Returns a negative integer, zero, or a positive integer as the receiver is less than, equal
	// to, or greater than the parameter i.e. v1.CompareTo(v2) returns -1 if v1 < v2
	CompareTo(v Value) (int, error)
}

// Value is our value object that implements comparable so that two values can be compared.
type Value interface {
	Comparable

	// AsInterface to return the value as interface
	AsInterface() interface{}
}

func NewValueFromByte(value []byte, dataType jsonparser.ValueType) (Value, error) {
	switch dataType {
	case jsonparser.Boolean:
		b, err := strconv.ParseBool(string(value))
		if err != nil {
			return nil, err
		}
		return NewBoolValue(b), nil
	case jsonparser.Number:
		val, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return nil, err
		}

		if isIntegral(val) {
			return NewIntValue(int64(val)), nil
		}
		return NewDoubleValue(val), nil
	case jsonparser.String:
		return NewStringValue(string(value)), nil
	}

	return nil, status.Errorf(codes.InvalidArgument, "unsupported value type")
}

// NewValue returns Value object if it is able to create otherwise nil at this point the caller ensures that
// structpb.Value can be used to create internal value.
func NewValue(input *structpb.Value) Value {
	switch ty := input.Kind.(type) {
	case *structpb.Value_NumberValue:
		if isIntegral(ty.NumberValue) {
			return NewIntValue(int64(ty.NumberValue))
		} else {
			return NewDoubleValue(ty.NumberValue)
		}
	case *structpb.Value_StringValue:
		return NewStringValue(ty.StringValue)
	case *structpb.Value_BoolValue:
		return NewBoolValue(ty.BoolValue)
	}

	return nil
}

// NewValueUsingSchema returns Value object using Schema from the input struct Value
func NewValueUsingSchema(field *schema.Field, input *structpb.Value) (Value, error) {
	switch field.Type() {
	case schema.BoolType:
		if inpVal, ok := input.Kind.(*structpb.Value_BoolValue); ok {
			i := BoolValue(inpVal.BoolValue)
			return &i, nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "permissible type for '%s' is bool only", field.FieldName)
	case schema.IntType:
		if inpVal, ok := input.Kind.(*structpb.Value_NumberValue); ok {
			if isIntegral(inpVal.NumberValue) {
				return NewIntValue(int64(inpVal.NumberValue)), nil
			}
		}
		return nil, status.Errorf(codes.InvalidArgument, "permissible type for '%s' is int only", field.FieldName)
	case schema.DoubleType:
		if inpVal, ok := input.Kind.(*structpb.Value_NumberValue); ok {
			return NewDoubleValue(inpVal.NumberValue), nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "permissible type for '%s' is double only", field.FieldName)
	case schema.BytesType:
		if inpVal, ok := input.Kind.(*structpb.Value_StringValue); ok {
			if decoded, err := base64.StdEncoding.DecodeString(inpVal.StringValue); err == nil {
				return NewBytesValue(decoded), nil
			} else {
				return NewBytesValue([]byte(inpVal.StringValue)), nil
			}
		}
		return nil, status.Errorf(codes.InvalidArgument, "permissible type for '%s' is bytes only", field.FieldName)
	case schema.StringType:
		if inpVal, ok := input.Kind.(*structpb.Value_StringValue); ok {
			return NewStringValue(inpVal.StringValue), nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "permissible type for '%s' is string only", field.FieldName)
	}

	return nil, status.Errorf(codes.InvalidArgument, "unsupported type '%T'", input.Kind)
}

// NewValueFromStruct returns Value object based on the schema using input Struct
func NewValueFromStruct(field *schema.Field, inputS *structpb.Struct) (Value, error) {
	input, ok := inputS.GetFields()[field.FieldName]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "field is missing in the input")
	}

	return NewValueUsingSchema(field, input)
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
