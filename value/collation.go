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

package value

import (
	api "github.com/tigrisdata/tigris/api/server/v1"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

const (
	INDEX_MAX_STRING_LEN = 64
)

type Collation struct {
	collator     collate.Collator
	apiCollation *api.Collation
}

func NewCollation() *Collation {
	return NewCollationFrom(nil)
}

func NewSortKeyCollation() *Collation {
	return NewCollationFrom(&api.Collation{Case: "csk"})
}

func NewCollationFrom(apiCollation *api.Collation) *Collation {
	var options []collate.Option

	if apiCollation == nil {
		apiCollation = &api.Collation{}
	}

	if apiCollation.IsCaseInsensitive() {
		options = append(options, collate.IgnoreCase)
	}

	return &Collation{
		collator:     *collate.New(language.English, options...),
		apiCollation: apiCollation,
	}
}

// CompareString returns an integer comparing the two strings. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (c *Collation) CompareString(a string, b string) int {
	return c.collator.CompareString(a, b)
}

func (x *Collation) IsCaseSensitive() bool {
	return x.apiCollation.IsCaseSensitive()
}

func (x *Collation) IsCaseInsensitive() bool {
	return x.apiCollation.IsCaseInsensitive()
}

func (x *Collation) IsCollationSortKey() bool {
	return x.apiCollation.IsCollationSortKey()
}

func (x *Collation) IsValid() error {
	return x.apiCollation.IsValid()
}

func (x *Collation) GenerateSortKey(input string) []byte {
	// Only index up to MAX_STRING_LEN of the string
	if len(input) > INDEX_MAX_STRING_LEN {
		input = input[:INDEX_MAX_STRING_LEN]
	}

	var buf collate.Buffer
	collated := x.collator.KeyFromString(&buf, input)
	return collated
}
