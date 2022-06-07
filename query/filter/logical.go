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
)

type LogicalOP string

const (
	AndOP LogicalOP = "$and"
	OrOP  LogicalOP = "$or"
)

// LogicalFilter (or boolean) are the filters that evaluates to True or False. A logical operator can have the following
// form inside the JSON
//    {"$and": [{"f1":1}, {"f2": 3}]}
//    {"$or": [{"f1":1}, {"f2": 3}]}
type LogicalFilter interface {
	GetFilters() []Filter
	Type() LogicalOP
}

// AndFilter performs a logical AND operation on an array of two or more expressions. The and filter looks like this,
// {"$and": [{"f1":1}, {"f2": 3}....]}
// It can be nested i.e. a top level $and can have multiple nested $and/$or
type AndFilter struct {
	filter []Filter
}

func NewAndFilter(filter []Filter) (*AndFilter, error) {
	a := &AndFilter{
		filter: filter,
	}

	if err := a.validate(); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *AndFilter) validate() error {
	if len(a.filter) < 2 {
		return fmt.Errorf("and filter needs minimum 2 filters")
	}

	return nil
}

func (a *AndFilter) Type() LogicalOP {
	return AndOP
}

// Matches returns true if the input doc matches this filter.
func (a *AndFilter) Matches(doc []byte) bool {
	for _, f := range a.filter {
		if !f.Matches(doc) {
			return false
		}
	}

	return true
}

// GetFilters returns all the nested filters for AndFilter
func (a *AndFilter) GetFilters() []Filter {
	return a.filter
}

func (a *AndFilter) ToSearchFilter() string {
	var str string
	for i, f := range a.filter {
		str += f.ToSearchFilter()
		if i < len(a.filter)-1 {
			str += "&&"
		}
	}
	return str
}

// String a helpful method for logging.
func (a *AndFilter) String() string {
	var str = "{$and"
	for _, f := range a.filter {
		str += fmt.Sprintf("%s", f)
	}
	return str + "}"
}

// OrFilter performs a logical OR operation on an array of two or more expressions. The or filter looks like this,
// {"$or": [{"f1":1}, {"f2": 3}....]}
// It can be nested i.e. a top level "$or" can have multiple nested $and/$or
type OrFilter struct {
	filter []Filter
}

func NewOrFilter(filter []Filter) (*OrFilter, error) {
	o := &OrFilter{
		filter: filter,
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	return o, nil
}

func (o *OrFilter) validate() error {
	if len(o.filter) < 2 {
		return fmt.Errorf("or filter needs minimum 2 filters")
	}

	return nil
}

func (o *OrFilter) Type() LogicalOP {
	return OrOP
}

// Matches returns true if the input doc matches this filter.
func (o *OrFilter) Matches(doc []byte) bool {
	for _, f := range o.filter {
		if f.Matches(doc) {
			return true
		}
	}

	return false
}

// GetFilters returns all the nested filters for OrFilter
func (o *OrFilter) GetFilters() []Filter {
	return o.filter
}

func (o *OrFilter) ToSearchFilter() string {
	var str string
	for i, f := range o.filter {
		str += f.ToSearchFilter()
		if i < len(o.filter)-1 {
			str += ""
		}
	}
	return str
}

// String a helpful method for logging.
func (o *OrFilter) String() string {
	var str = "{$or:"
	for _, f := range o.filter {
		str += fmt.Sprintf("%s", f)
	}
	return str + "}"
}
