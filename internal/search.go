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

package internal

type SearchField struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Locale     *string `json:"locale,omitempty"`
	Drop       *bool   `json:"drop,omitempty"`
	Facet      *bool   `json:"facet,omitempty"`
	Index      *bool   `json:"index,omitempty"`
	Infix      *bool   `json:"infix,omitempty"`
	Optional   *bool   `json:"optional,omitempty"`
	Sort       *bool   `json:"sort,omitempty"`
	Dimensions *int32  `json:"num_dim,omitempty"`
}

type SearchIndexSchema struct {
	Name           string        `json:"name,omitempty"`
	Fields         []SearchField `json:"fields"`
	DefaultSorting *string       `json:"default_sorting_field,omitempty"`
	EnableNested   *bool         `json:"enable_nested_fields,omitempty"`
}

type SearchIndexResponse struct {
	SearchIndexSchema
	CreatedAt    int64 `json:"created_at"`
	NumDocuments int64 `json:"num_documents"`
}
