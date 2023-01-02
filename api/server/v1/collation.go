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

package api

const CollationKey string = "collation"

type CollationType uint8

const (
	Undefined CollationType = iota
	CaseInsensitive
	CaseSensitive
)

var SupportedCollations = [...]string{
	CaseInsensitive: "ci",
	CaseSensitive:   "cs",
}

func (x *Collation) IsCaseSensitive() bool {
	return x.Case == SupportedCollations[CaseSensitive]
}

func (x *Collation) IsCaseInsensitive() bool {
	return x.Case == SupportedCollations[CaseInsensitive]
}

func (x *Collation) IsValid() error {
	if caseType := ToCollationType(x.Case); caseType == Undefined {
		return Errorf(Code_INVALID_ARGUMENT, "collation '%s' is not supported", x.Case)
	}

	return nil
}

func ToCollationType(userCollation string) CollationType {
	for ct, cs := range SupportedCollations {
		if cs == userCollation {
			return CollationType(ct)
		}
	}

	return Undefined
}
