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

import (
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type DocMetadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	Match     *Match     `json:"match,omitempty"`
}

func (x *IndexDoc) MarshalJSON() ([]byte, error) {
	if x.Doc == nil {
		return []byte("null"), nil
	}

	resp := struct {
		Doc      jsoniter.RawMessage `json:"doc,omitempty"`
		Metadata *DocMetadata        `json:"metadata,omitempty"`
	}{
		Doc:      x.Doc,
		Metadata: CreateMDFromDocMeta(x.Metadata),
	}

	return jsoniter.Marshal(resp)
}

func CreateMDFromDocMeta(x *DocMeta) *DocMetadata {
	if x == nil {
		return nil
	}

	var md DocMetadata
	if x.CreatedAt != nil {
		tm := x.CreatedAt.AsTime()
		md.CreatedAt = &tm
	}
	if x.UpdatedAt != nil {
		tm := x.UpdatedAt.AsTime()
		md.UpdatedAt = &tm
	}
	md.Match = x.Match

	return &md
}

func (x *GetDocumentResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Documents []*IndexDoc `json:"documents,omitempty"`
	}{
		Documents: x.Documents,
	}

	return jsoniter.Marshal(resp)
}

func (x *DocStatus) MarshalJSON() ([]byte, error) {
	resp := struct {
		ID    string `json:"id,omitempty"`
		Error *Error `json:"error"`
	}{
		ID:    x.Id,
		Error: x.Error,
	}
	return jsoniter.Marshal(resp)
}

func (x *IndexInfo) MarshalJSON() ([]byte, error) {
	resp := struct {
		Name   string              `json:"name,omitempty"`
		Schema jsoniter.RawMessage `json:"schema,omitempty"`
	}{
		Name:   x.Name,
		Schema: x.Schema,
	}
	return jsoniter.Marshal(resp)
}

func (x *CreateOrUpdateIndexRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}
		switch strings.ToLower(key) {
		case "project":
			v = &x.Project
		case "name":
			v = &x.Name
		case "schema":
			x.Schema = value
			continue
		default:
			continue
		}
		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *CreateByIdRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage

	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "id":
			v = &x.Id
		case "document":
			x.Document = value
			continue
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *CreateDocumentRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage

	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "documents":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Documents = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Documents[i] = docs[i]
			}
			continue
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *CreateOrReplaceDocumentRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage

	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "documents":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Documents = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Documents[i] = docs[i]
			}
			continue
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *UpdateDocumentRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "documents":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Documents = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Documents[i] = docs[i]
			}
			continue
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}
	return nil
}

func (x *DeleteDocumentRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "ids":
			v = &x.Ids
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}
	return nil
}

func (x *DeleteByQueryRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
			continue
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}
	return nil
}

func (x *SearchIndexRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage

	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}

		switch key {
		case "project":
			v = &x.Project
		case "index":
			v = &x.Index
		case "search_fields":
			v = &x.SearchFields
		case "q":
			v = &x.Q
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
			continue
		case "facet":
			// delaying the facet deserialization to dedicated handler
			x.Facet = value
			continue
		case "sort":
			// delaying the sort deserialization
			x.Sort = value
			continue
		case "include_fields":
			v = &x.IncludeFields
		case "exclude_fields":
			v = &x.ExcludeFields
		case "page_size":
			v = &x.PageSize
		case "page":
			v = &x.Page
		case "collation":
			v = &x.Collation
		default:
			continue
		}

		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}
