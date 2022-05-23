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

package api

import (
	"encoding/json"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	jsoniter "github.com/json-iterator/go"
	spb "google.golang.org/genproto/googleapis/rpc/status"
)

// CustomMarshaler is a marshaler to customize the response. Currently it is only used to marshal custom error message
// otherwise it just use the inbuilt mux marshaller.
type CustomMarshaler struct {
	*runtime.JSONBuiltin
}

func (c *CustomMarshaler) Marshal(v interface{}) ([]byte, error) {
	switch ty := v.(type) {
	case *spb.Status:
		return MarshalStatus(ty)
	case *ListCollectionsResponse:
		if len(ty.Collections) == 0 {
			return []byte(`{"collections":[]}`), nil
		}
		return c.JSONBuiltin.Marshal(v)
	case *ListDatabasesResponse:
		if len(ty.Databases) == 0 {
			return []byte(`{"databases":[]}`), nil
		}
		return c.JSONBuiltin.Marshal(v)
	}
	return c.JSONBuiltin.Marshal(v)
}

// MarshalJSON on read response avoid any encoding/decoding on x.Data. With this approach we are not doing any extra
// marshaling/unmarshalling in returning the data from the database. The document returned from the database is stored
// in x.Data and will return as-is.
//
// Note: This also means any changes in ReadResponse proto needs to make sure that we add that here and similarly
// the openAPI specs needs to be specified Data as object instead of bytes.
func (x *ReadResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Data        json.RawMessage `json:"data,omitempty"`
		Metadata    Metadata        `json:"metadata,omitempty"`
		ResumeToken []byte          `json:"resume_token,omitempty"`
	}{
		Data:        x.Data,
		Metadata:    CreateMDFromResponseMD(x.Metadata),
		ResumeToken: x.ResumeToken,
	}
	return json.Marshal(resp)
}

type Metadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func CreateMDFromResponseMD(x *ResponseMetadata) Metadata {
	var md Metadata
	if x.CreatedAt != nil {
		tm := x.CreatedAt.AsTime()
		md.CreatedAt = &tm
	}
	if x.UpdatedAt != nil {
		tm := x.UpdatedAt.AsTime()
		md.UpdatedAt = &tm
	}
	if x.DeletedAt != nil {
		tm := x.DeletedAt.AsTime()
		md.DeletedAt = &tm
	}

	return md
}

// UnmarshalJSON on ReadRequest avoids unmarshalling filter and instead this way we can write a custom struct to do
// the unmarshalling and will be avoiding any extra allocation/copying.
func (x *ReadRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
		case "fields":
			// not decoding it here and let it decode during fields parsing
			x.Fields = value
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on InsertRequest avoids unmarshalling user document. We only need to extract primary/index keys from
// the document and want to store the document as-is in the database. This way there is no extra cost of serialization/deserialization
// and also less error-prone because we are not touching the user document. The req handler needs to extract out
// the relevant keys from the user docs and should pass it as-is to the underlying engine.
func (x *InsertRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "documents":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Documents = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Documents[i] = docs[i]
			}
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on ReplaceRequest avoids unmarshalling user document. We only need to extract primary/index keys from
// the document and want to store the document as-is in the database. This way there is no extra cost of serialization/deserialization
// and also less error-prone because we are not touching the user document. The req handler needs to extract out
// the relevant keys from the user docs and should pass it as-is to the underlying engine.
func (x *ReplaceRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "documents":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Documents = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Documents[i] = docs[i]
			}
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on UpdateRequest avoids unmarshalling filter and instead this way we can write a custom struct to do
// the unmarshalling and will be avoiding any extra allocation/copying.
func (x *UpdateRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "fields":
			// not decoding it here and let it decode during Fields parsing
			x.Fields = value
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on DeleteRequest avoids unmarshalling filter and instead this way we can write a custom struct to do
// the unmarshalling and will be avoiding any extra allocation/copying.
func (x *DeleteRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on CreateCollectionRequest avoids unmarshalling schema. The req handler deserializes the schema.
func (x *CreateOrUpdateCollectionRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "only_create":
			if err := jsoniter.Unmarshal(value, &x.OnlyCreate); err != nil {
				return err
			}
		case "schema":
			x.Schema = value
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

type collDesc struct {
	Collection string              `json:"collection"`
	Metadata   *CollectionMetadata `json:"metadata"`
	Schema     json.RawMessage     `json:"schema"`
}

func (x *DescribeCollectionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&collDesc{
		Collection: x.Collection,
		Metadata:   x.Metadata,
		Schema:     x.Schema,
	})
}

func (x *DescribeDatabaseResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Db          string            `json:"db"`
		Metadata    *DatabaseMetadata `json:"metadata"`
		Collections []*collDesc       `json:"collections"`
	}{
		Db:       x.Db,
		Metadata: x.Metadata,
	}

	for _, v := range x.Collections {
		resp.Collections = append(resp.Collections, &collDesc{
			Collection: v.Collection,
			Metadata:   v.Metadata,
			Schema:     v.Schema,
		})
	}

	return json.Marshal(&resp)
}

func (x *StreamResponse) MarshalJSON() ([]byte, error) {
	type event struct {
		TxId       []byte          `json:"tx_id"`
		Collection string          `json:"collection"`
		Op         string          `json:"op"`
		Key        []byte          `json:"key,omitempty"`
		LKey       []byte          `json:"lkey,omitempty"`
		RKey       []byte          `json:"rkey,omitempty"`
		Data       json.RawMessage `json:"data,omitempty"`
		Last       bool            `json:"last"`
	}

	resp := struct {
		Event event `json:"event,omitempty"`
	}{
		Event: event{
			TxId:       x.Event.TxId,
			Collection: x.Event.Collection,
			Op:         x.Event.Op,
			Key:        x.Event.Key,
			LKey:       x.Event.Lkey,
			RKey:       x.Event.Rkey,
			Data:       x.Event.Data,
			Last:       x.Event.Last,
		},
	}
	return json.Marshal(resp)
}

// Proper marshal timestamp in metadata
type dmlResponse struct {
	Metadata      Metadata `json:"metadata,omitempty"`
	Status        string   `json:"status,omitempty"`
	ModifiedCount int32    `json:"modified_count,omitempty"`
}

func (x *InsertResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status})
}

func (x *ReplaceResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status})
}

func (x *DeleteResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status})
}

func (x *UpdateResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status, ModifiedCount: x.ModifiedCount})
}
