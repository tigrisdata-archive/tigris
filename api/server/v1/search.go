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

	jsoniter "github.com/json-iterator/go"
)

func (x *GetDocumentResponse) MarshalJSON() ([]byte, error) {
	type resp struct {
		Documents []jsoniter.RawMessage `json:"documents,omitempty"`
	}

	r := resp{}
	r.Documents = make([]jsoniter.RawMessage, len(x.Documents))
	for i, doc := range x.Documents {
		if len(doc) == 0 {
			r.Documents[i] = nil
			continue
		}

		r.Documents[i] = doc
	}

	return jsoniter.Marshal(r)
}

func (x *DocStatus) MarshalJSON() ([]byte, error) {
	resp := struct {
		Id    string `json:"id,omitempty"`
		Error *Error `json:"error"`
	}{
		Id:    x.Id,
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
