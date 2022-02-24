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

package filter

import (
	"fmt"

	structpb "github.com/gogo/protobuf/types"
)

const (
	EQ = "$eq"
	GT = "$gt"
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

	// Get to return the value
	Get() interface{}
}

// NewValue returns Value object if it is able to create otherwise nil at this point the caller ensures that
// json.Value can be used to create internal value.
func NewValue(input *structpb.Value) Value {
	switch ty := input.Kind.(type) {
	case *structpb.Value_NumberValue:
		i := IntValue(ty.NumberValue)
		return &i
	case *structpb.Value_StringValue:
		i := StringValue(ty.StringValue)
		return &i
	case *structpb.Value_BoolValue:
		i := BoolValue(ty.BoolValue)
		return &i
	}

	return nil
}

type IntValue int64

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

func (i *IntValue) Get() interface{} {
	return int64(*i)
}

func (i *IntValue) String() string {
	if i == nil {
		return ""
	}

	return fmt.Sprintf("%d", *i)
}

type StringValue string

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

func (s *StringValue) Get() interface{} {
	return string(*s)
}

func (s *StringValue) String() string {
	if s == nil {
		return ""
	}

	return string(*s)
}

type BoolValue bool

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

func (b *BoolValue) Get() interface{} {
	return bool(*b)
}

func (b *BoolValue) String() string {
	if b == nil {
		return ""
	}

	return fmt.Sprintf("%v", *b)
}

// ValueMatcher is an interface that has method like Matches.
type ValueMatcher interface {
	// Matches returns true if the receiver has the value object that has the same value as input
	Matches(input Value) bool

	// Type return the type of the value matcher, syntactic sugar for logging, etc
	Type() string

	// GetValue returns the value on which the Matcher is operating
	GetValue() Value
}

// NewMatcher returns ValueMatcher that is derived from the key
func NewMatcher(key string, value *structpb.Value) (ValueMatcher, error) {
	v := NewValue(value)

	switch key {
	case EQ:
		return &EqualityMatcher{
			Value: v,
		}, nil
	case GT:
		return &GreaterThanMatcher{
			Value: v,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operand %s", key)
	}
}

// EqualityMatcher implements "$eq" operand.
type EqualityMatcher struct {
	Value Value
}

// NewEqualityMatcher returns EqualityMatcher object
func NewEqualityMatcher(v Value) *EqualityMatcher {
	return &EqualityMatcher{
		Value: v,
	}
}

func (e *EqualityMatcher) GetValue() Value {
	return e.Value
}

func (e *EqualityMatcher) Matches(input Value) bool {
	res, _ := e.Value.CompareTo(input)
	return res == 0
}

func (e *EqualityMatcher) Type() string {
	return "$eq"
}

func (e *EqualityMatcher) String() string {
	return fmt.Sprintf("{$eq:%v}", e.Value)
}

// GreaterThanMatcher implements "$gt" operand.
type GreaterThanMatcher struct {
	Value Value
}

func NewGreaterThanMatcher(v Value) *GreaterThanMatcher {
	return &GreaterThanMatcher{
		Value: v,
	}
}

func (g *GreaterThanMatcher) GetValue() Value {
	return g.Value
}


func (g *GreaterThanMatcher) Matches(input Value) bool {
	res, _ := g.Value.CompareTo(input)
	return res > 0
}

func (g *GreaterThanMatcher) Type() string {
	return "$gt"
}

func (g *GreaterThanMatcher) String() string {
	return fmt.Sprintf("{$gt:%v}", g.Value)

}
