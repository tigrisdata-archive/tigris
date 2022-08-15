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

	"github.com/buger/jsonparser"
	"github.com/tigrisdata/tigris/schema"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
	"github.com/tigrisdata/tigris/lib/date"
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
	Field   *schema.QueryableField
	Matcher ValueMatcher
}

// NewSelector returns Selector object
func NewSelector(field *schema.QueryableField, matcher ValueMatcher) *Selector {
	return &Selector{
		Field:   field,
		Matcher: matcher,
	}
}

func (s *Selector) MatchesDoc(doc map[string]interface{}) bool {
	v, ok := doc[s.Field.Name()]
	if !ok {
		return true
	}

	var val value.Value
	switch s.Field.DataType {
	case schema.StringType:
		val = value.NewStringValue(v.(string))
	case schema.DoubleType:
		val = value.NewDoubleUsingFloat(v.(float64))
	default:
		// as this method is only intended for indexing store, so we only apply filter for string and double type
		// otherwise we rely on indexing store to only return valid results.
		return true
	}

	return s.Matcher.Matches(val)
}

// Matches returns true if the input doc matches this filter.
func (s *Selector) Matches(doc []byte) bool {
	docValue, dtp, _, err := jsonparser.Get(doc, s.Field.Name())
	if ulog.E(err) {
		return false
	}
	if dtp == jsonparser.NotExist {
		return false
	}

	val, err := value.NewValue(s.Field.DataType, docValue)
	if ulog.E(err) {
		return false
	}

	return s.Matcher.Matches(val)
}

func (s *Selector) ToSearchFilter() []string {
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

	v := s.Matcher.GetValue()
	switch s.Field.DataType {
	case schema.DoubleType:
		// for double, we pass string in the filter to search backend
		return []string{fmt.Sprintf(op, s.Field.Name(), v.String())}
	case schema.DateTimeType:
		// encode into int64
		if nsec, err := date.ToUnixNano(schema.DateTimeFormat, v.String()); err == nil {
			return []string{fmt.Sprintf(op, s.Field.Name(), nsec)}
		}

	}
	return []string{fmt.Sprintf(op, s.Field.Name(), v.AsInterface())}
}

// String a helpful method for logging.
func (s *Selector) String() string {
	return fmt.Sprintf("{%v:%v}", s.Field.Name(), s.Matcher)
}
