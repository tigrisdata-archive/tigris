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
//
//	{"$and": [{"f1":1}, {"f2": 3}]}
//	{"$or": [{"f1":1}, {"f2": 3}]}
type LogicalFilter interface {
	GetFilters() []Filter
	Type() LogicalOP
	Filter
}

// AndFilter performs a logical AND operation on an array of two or more expressions. The and filter looks like this,
// {"$and": [{"f1":1}, {"f2": 3}....]}
// It can be nested i.e. a top level $and can have multiple nested $and/$or.
type AndFilter struct {
	filter []Filter
	searchSerializer
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
func (a *AndFilter) Matches(doc []byte, metadata []byte) bool {
	for _, f := range a.filter {
		if !f.Matches(doc, metadata) {
			return false
		}
	}

	return true
}

func (a *AndFilter) MatchesDoc(doc map[string]interface{}) bool {
	for _, f := range a.filter {
		if !f.MatchesDoc(doc) {
			return false
		}
	}

	return true
}

// GetFilters returns all the nested filters for AndFilter.
func (a *AndFilter) GetFilters() []Filter {
	return a.filter
}

// String a helpful method for logging.
func (a *AndFilter) String() string {
	str := "{$and"
	for _, f := range a.filter {
		str += fmt.Sprintf("%s", f)
	}
	return str + "}"
}

func (a *AndFilter) ToSearchFilter() string {
	return a.serialize(" && ", a.filter)[0]
}

func (a *AndFilter) IsSearchIndexed() bool {
	for _, f := range a.filter {
		if !f.IsSearchIndexed() {
			return false
		}
	}

	return true
}

// OrFilter performs a logical OR operation on an array of two or more expressions. The or filter looks like this,
// {"$or": [{"f1":1}, {"f2": 3}....]}
// It can be nested i.e. a top level "$or" can have multiple nested $and/$or.
type OrFilter struct {
	filter []Filter
	searchSerializer
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
func (o *OrFilter) Matches(doc []byte, metadata []byte) bool {
	for _, f := range o.filter {
		if f.Matches(doc, metadata) {
			return true
		}
	}

	return false
}

func (o *OrFilter) MatchesDoc(doc map[string]interface{}) bool {
	for _, f := range o.filter {
		if f.MatchesDoc(doc) {
			return true
		}
	}

	return false
}

// GetFilters returns all the nested filters for OrFilter.
func (o *OrFilter) GetFilters() []Filter {
	return o.filter
}

func (o *OrFilter) ToSearchFilter() string {
	return o.serialize(" || ", o.filter)[0]
}

func (o *OrFilter) IsSearchIndexed() bool {
	for _, f := range o.filter {
		if !f.IsSearchIndexed() {
			return false
		}
	}

	return true
}

// String a helpful method for logging.
func (o *OrFilter) String() string {
	str := "{$or:"
	for _, f := range o.filter {
		str += fmt.Sprintf("%s", f)
	}
	return str + "}"
}

type searchSerializer struct{}

func (sz *searchSerializer) serialize(searchToken string, filters []Filter) []string {
	var selectors []*Selector
	var logical []LogicalFilter
	for _, f := range filters {
		switch conv := f.(type) {
		case *Selector:
			selectors = append(selectors, conv)
		case LogicalFilter:
			logical = append(logical, f.(LogicalFilter))
		}
	}

	var str string
	for i, s := range selectors {
		// first "&&" all selectors
		if i != 0 {
			str += searchToken
		}
		str += s.ToSearchFilter()
	}

	var flattened []string
	if len(logical) > 0 {
		flattened = sz.flatten(searchToken, str, logical)
	}
	if len(flattened) == 0 {
		flattened = append(flattened, str)
	}
	return flattened
}

func (sz *searchSerializer) flatten(token string, soFar string, filters []LogicalFilter) []string {
	var combs []string

	e := filters[0].ToSearchFilter()
	temp := soFar
	if len(temp) > 0 {
		temp = temp + token + "(" + e + ")"
	} else {
		temp = e
	}

	if len(filters) > 1 {
		combs = append(combs, sz.flatten(token, temp, filters[1:])...)
	} else {
		combs = append(combs, temp)
	}

	return combs
}
