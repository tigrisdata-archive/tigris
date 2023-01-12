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

package schema

const (
	SearchId = "id"
)

type ReservedField uint8

const (
	CreatedAt ReservedField = iota
	UpdatedAt
	Metadata
	IdToSearchKey
	DateSearchKeyPrefix
	SearchArrNullItem
)

var ReservedFields = [...]string{
	CreatedAt:           "created_at",
	UpdatedAt:           "updated_at",
	Metadata:            "metadata",
	IdToSearchKey:       "_tigris_id",
	DateSearchKeyPrefix: "_tigris_date_",
	SearchArrNullItem:   "_tigris_null",
}

func IsReservedField(name string) bool {
	for _, r := range ReservedFields {
		if r == name {
			return true
		}
	}

	return false
}

func IsSearchID(name string) bool {
	return name == SearchId
}

// ToSearchDateKey can be used to generate storage field for search backend
// Original date strings are persisted as it is under this field.
func ToSearchDateKey(key string) string {
	return ReservedFields[DateSearchKeyPrefix] + key
}
