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

// Selector is a condition defined inside a filter. It has a field which corresponding the field on which condition
// is defined and then Matcher. A matcher is formed from the user condition i.e. if the condition is on "$eq" then
// a EqualityMatcher is created. The matcher implements a Match method which is used to know if the input document
// passes the condition.
//
// A Selector can have this form inside the input JSON
//    {f:{$eq:1}}
//    {f:20} (default is "$eq" so we automatically append EqualityMatcher for this case in parser)
//    {f:<Expr>}
type Selector struct {
	Field   string
	Matcher ValueMatcher
}

// NewSelector returns Selector object
func NewSelector(field string, matcher ValueMatcher) *Selector {
	return &Selector{
		Field:   field,
		Matcher: matcher,
	}
}

// Matches returns true if the input doc matches this filter.
func (s *Selector) Matches(doc []byte) bool {
	//ToDo: not implemented
	return false
}

func (s *Selector) ToSearchFilter() string {
	var op string
	switch s.Matcher.Type() {
	case EQ:
		op = "%s:=%v"
	case GT:
		op = "%s:>%v"
	case GTE:
		op = "%s:>=%v"
	case LT:
		op = "%s:<%v"
	case LTE:
		op = "%s:<=%v"
	}
	return fmt.Sprintf(op, s.Field, s.Matcher.GetValue().AsInterface())
}

// String a helpful method for logging.
func (s *Selector) String() string {
	return fmt.Sprintf("{%v:%v}", s.Field, s.Matcher)
}
