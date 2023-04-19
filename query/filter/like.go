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

	"github.com/buger/jsonparser"
	"github.com/tigrisdata/tigris/schema"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// LikeFilter creates a filter that offers "like" semantics i.e. "regex"/"contains"/"not". It is not used to create
// any key. It is always used to post-process the records.
type LikeFilter struct {
	Field   *schema.QueryableField
	Matcher LikeMatcher
}

func NewLikeFilter(field *schema.QueryableField, matcher LikeMatcher) *LikeFilter {
	return &LikeFilter{
		Field:   field,
		Matcher: matcher,
	}
}

func (s *LikeFilter) MatchesDoc(doc map[string]any) bool {
	v, ok := doc[s.Field.Name()]
	if !ok {
		return true
	}

	return s.Matcher.Matches(v)
}

// Matches returns true if the doc value matches this filter.
func (s *LikeFilter) Matches(doc []byte) bool {
	docValue, dtp, _, err := jsonparser.Get(doc, s.Field.KeyPath()...)
	if dtp == jsonparser.NotExist {
		return false
	}
	if ulog.E(err) {
		return false
	}
	if dtp == jsonparser.Null {
		docValue = nil
	}

	return s.Matcher.Matches(docValue)
}

func (s *LikeFilter) ToSearchFilter() string {
	return ""
}

func (s *LikeFilter) IsSearchIndexed() bool {
	return false
}

func (s *LikeFilter) String() string {
	return fmt.Sprintf("{%v:%v}", s.Field.Name(), s.Matcher)
}
