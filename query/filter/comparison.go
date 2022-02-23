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

	"github.com/tigrisdata/tigrisdb/value"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/types/known/structpb"
)

const (
	EQ = "$eq"
	GT = "$gt"
)

// ValueMatcher is an interface that has method like Matches.
type ValueMatcher interface {
	// Matches returns true if the receiver has the value object that has the same value as input
	Matches(input value.Value) bool

	// Type return the type of the value matcher, syntactic sugar for logging, etc
	Type() string

	// GetValue returns the value on which the Matcher is operating
	GetValue() value.Value
}

// NewMatcher returns ValueMatcher that is derived from the key
func NewMatcher(key string, userValue *structpb.Value) (ValueMatcher, error) {
	v := value.NewValue(userValue)

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
		return nil, status.Errorf(codes.InvalidArgument, "unsupported operand '%s'", key)
	}
}

// EqualityMatcher implements "$eq" operand.
type EqualityMatcher struct {
	Value value.Value
}

// NewEqualityMatcher returns EqualityMatcher object
func NewEqualityMatcher(v value.Value) *EqualityMatcher {
	return &EqualityMatcher{
		Value: v,
	}
}

func (e *EqualityMatcher) GetValue() value.Value {
	return e.Value
}

func (e *EqualityMatcher) Matches(input value.Value) bool {
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
	Value value.Value
}

func NewGreaterThanMatcher(v value.Value) *GreaterThanMatcher {
	return &GreaterThanMatcher{
		Value: v,
	}
}

func (g *GreaterThanMatcher) GetValue() value.Value {
	return g.Value
}

func (g *GreaterThanMatcher) Matches(input value.Value) bool {
	res, _ := g.Value.CompareTo(input)
	return res > 0
}

func (g *GreaterThanMatcher) Type() string {
	return "$gt"
}

func (g *GreaterThanMatcher) String() string {
	return fmt.Sprintf("{$gt:%v}", g.Value)

}
