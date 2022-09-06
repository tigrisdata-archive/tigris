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

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/value"
)

const (
	EQ  = "$eq"
	GT  = "$gt"
	LT  = "$lt"
	GTE = "$gte"
	LTE = "$lte"
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
func NewMatcher(key string, v value.Value) (ValueMatcher, error) {
	switch key {
	case EQ:
		return &EqualityMatcher{
			Value: v,
		}, nil
	case GT:
		return &GreaterThanMatcher{
			Value: v,
		}, nil
	case GTE:
		return &GreaterThanEqMatcher{
			Value: v,
		}, nil
	case LT:
		return &LessThanMatcher{
			Value: v,
		}, nil
	case LTE:
		return &LessThanEqMatcher{
			Value: v,
		}, nil
	default:
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported operand '%s'", key)
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
	res, _ := input.CompareTo(e.Value)
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

func (g *GreaterThanMatcher) GetValue() value.Value {
	return g.Value
}

func (g *GreaterThanMatcher) Matches(input value.Value) bool {
	res, _ := input.CompareTo(g.Value)
	return res > 0
}

func (g *GreaterThanMatcher) Type() string {
	return "$gt"
}

func (g *GreaterThanMatcher) String() string {
	return fmt.Sprintf("{$gt:%v}", g.Value)
}

// GreaterThanEqMatcher implements "$gte" operand.
type GreaterThanEqMatcher struct {
	Value value.Value
}

func (g *GreaterThanEqMatcher) GetValue() value.Value {
	return g.Value
}

func (g *GreaterThanEqMatcher) Matches(input value.Value) bool {
	res, _ := input.CompareTo(g.Value)
	return res >= 0
}

func (g *GreaterThanEqMatcher) Type() string {
	return "$gte"
}

func (g *GreaterThanEqMatcher) String() string {
	return fmt.Sprintf("{$gte:%v}", g.Value)
}

// LessThanMatcher implements "$lt" operand.
type LessThanMatcher struct {
	Value value.Value
}

func (l *LessThanMatcher) GetValue() value.Value {
	return l.Value
}

func (l *LessThanMatcher) Matches(input value.Value) bool {
	res, _ := input.CompareTo(l.Value)
	return res < 0
}

func (l *LessThanMatcher) Type() string {
	return "$lt"
}

func (l *LessThanMatcher) String() string {
	return fmt.Sprintf("{$lt:%v}", l.Value)
}

// LessThanEqMatcher implements "$lte" operand.
type LessThanEqMatcher struct {
	Value value.Value
}

func (l *LessThanEqMatcher) GetValue() value.Value {
	return l.Value
}

func (l *LessThanEqMatcher) Matches(input value.Value) bool {
	res, _ := input.CompareTo(l.Value)
	return res <= 0
}

func (l *LessThanEqMatcher) Type() string {
	return "$lte"
}

func (l *LessThanEqMatcher) String() string {
	return fmt.Sprintf("{$lte:%v}", l.Value)
}
