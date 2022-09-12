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
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	jsoniter "github.com/json-iterator/go"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	refreshToken = "refresh_token"
)

type CustomDecoder struct {
	jsonDecoder *json.Decoder
	reader      io.Reader
}

func (f CustomDecoder) Decode(dst interface{}) error {
	switch dst.(type) {
	case *GetAccessTokenRequest:
		{
			byteArr, err := io.ReadAll(f.reader)
			if err != nil {
				return err
			}
			return unmarshalInternal(byteArr, dst)
		}
	}
	return f.jsonDecoder.Decode(dst)
}

// CustomMarshaler is a marshaler to customize the response. Currently, it is only used to marshal custom error message
// otherwise it just uses the inbuilt mux marshaller.
type CustomMarshaler struct {
	JSONBuiltin *runtime.JSONBuiltin
}

func (c *CustomMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return CustomDecoder{
		jsonDecoder: json.NewDecoder(r),
		reader:      r,
	}
}

func (c *CustomMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return c.JSONBuiltin.NewEncoder(w)
}

func (c *CustomMarshaler) ContentType(v interface{}) string {
	return c.JSONBuiltin.ContentType(v)
}

func (c *CustomMarshaler) Marshal(v interface{}) ([]byte, error) {
	switch ty := v.(type) {
	case map[string]proto.Message:
		// this comes from GRPC-gateway streaming code
		if e, ok := ty["error"]; ok {
			return MarshalStatus(e.(*spb.Status))
		}
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

func (c *CustomMarshaler) Unmarshal(data []byte, v interface{}) error {
	switch v.(type) {
	case *GetAccessTokenRequest:
		{
			return unmarshalInternal(data, v)
		}
	}
	return c.JSONBuiltin.Unmarshal(data, v)
}

func unmarshalInternal(data []byte, v interface{}) error {
	switch v := v.(type) {
	case *GetAccessTokenRequest:
		{
			values, err := url.ParseQuery(string(data))
			if err != nil {
				return err
			}
			grantType := strings.ToUpper(values.Get("grant_type"))

			if grantType == GrantType_REFRESH_TOKEN.String() {
				v.GrantType = GrantType_REFRESH_TOKEN
				v.RefreshToken = values.Get(refreshToken)
			} else if grantType == GrantType_CLIENT_CREDENTIALS.String() {
				v.GrantType = GrantType_CLIENT_CREDENTIALS
				v.ClientId = values.Get("client_id")
				v.ClientSecret = values.Get("client_secret")
			}
			return nil
		}
	}
	return Errorf(Code_INTERNAL, "not supported")
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

// UnmarshalJSON for SearchRequest avoids unmarshalling filter, facets, sort and fields
func (x *SearchRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return nil
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
		case "search_fields":
			if err := jsoniter.Unmarshal(value, &x.SearchFields); err != nil {
				return err
			}
		case "q":
			if err := jsoniter.Unmarshal(value, &x.Q); err != nil {
				return err
			}
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
		case "facet":
			// delaying the facet deserialization to dedicated handler
			x.Facet = value
		case "sort":
			// delaying the sort deserialization
			x.Sort = value
		case "include_fields":
			if err := jsoniter.Unmarshal(value, &x.IncludeFields); err != nil {
				return err
			}
		case "exclude_fields":
			if err := jsoniter.Unmarshal(value, &x.ExcludeFields); err != nil {
				return err
			}
		case "page_size":
			if err := jsoniter.Unmarshal(value, &x.PageSize); err != nil {
				return err
			}
		case "page":
			if err := jsoniter.Unmarshal(value, &x.Page); err != nil {
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

// UnmarshalJSON on QueryTimeSeriesMetricsRequest. Handles enum.
func (x *QueryTimeSeriesMetricsRequest) UnmarshalJSON(data []byte) error {
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
		case "from":
			if err := jsoniter.Unmarshal(value, &x.From); err != nil {
				return err
			}
		case "to":
			if err := jsoniter.Unmarshal(value, &x.To); err != nil {
				return err
			}
		case "metric_name":
			if err := jsoniter.Unmarshal(value, &x.MetricName); err != nil {
				return err
			}
		case "tigris_operation":
			var t string
			if err := jsoniter.Unmarshal(value, &t); err != nil {
				return err
			}
			switch strings.ToUpper(t) {
			case "ALL":
				x.TigrisOperation = TigrisOperation_ALL
			case "READ":
				x.TigrisOperation = TigrisOperation_READ
			case "WRITE":
				x.TigrisOperation = TigrisOperation_WRITE
			}
		case "space_aggregation":
			var t string
			if err := jsoniter.Unmarshal(value, &t); err != nil {
				return err
			}
			switch strings.ToUpper(t) {
			case "AVG":
				x.SpaceAggregation = MetricQuerySpaceAggregation_AVG
			case "MIN":
				x.SpaceAggregation = MetricQuerySpaceAggregation_MIN
			case "MAX":
				x.SpaceAggregation = MetricQuerySpaceAggregation_MAX
			case "SUM":
				x.SpaceAggregation = MetricQuerySpaceAggregation_SUM
			}
		case "space_aggregated_by":
			if err := jsoniter.Unmarshal(value, &x.SpaceAggregatedBy); err != nil {
				return err
			}
		case "function":
			var t string
			if err := jsoniter.Unmarshal(value, &t); err != nil {
				return err
			}
			switch strings.ToUpper(t) {
			case "RATE":
				x.Function = MetricQueryFunction_RATE
			case "COUNT":
				x.Function = MetricQueryFunction_COUNT
			case "NONE":
				x.Function = MetricQueryFunction_NONE
			}
		case "additional_functions":
			var additionalFunctionRaw []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &additionalFunctionRaw); err != nil {
				return err
			}

			x.AdditionalFunctions = []*AdditionalFunction{}
			for i := 0; i < len(additionalFunctionRaw); i++ {
				additionalFunc, err := unmarshalAdditionalFunction(additionalFunctionRaw[i])
				if err != nil {
					return err
				}
				x.AdditionalFunctions = append(x.AdditionalFunctions, additionalFunc)
			}
		}
	}
	return nil
}

// UnmarshalJSON on GetAccessTokenRequest. Handles enum.
func (x *GetAccessTokenRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "grant_type":
			var grant string
			if err := jsoniter.Unmarshal(value, &grant); err != nil {
				return err
			}
			switch strings.ToUpper(grant) {
			case "REFRESH_TOKEN":
				x.GrantType = GrantType_REFRESH_TOKEN
			case "CLIENT_CREDENTIALS":
				x.GrantType = GrantType_CLIENT_CREDENTIALS
			}
		case "refresh_token":
			if err := jsoniter.Unmarshal(value, &x.RefreshToken); err != nil {
				return err
			}
		case "client_id":
			if err := jsoniter.Unmarshal(value, &x.ClientId); err != nil {
				return err
			}
		case "client_secret":
			if err := jsoniter.Unmarshal(value, &x.ClientSecret); err != nil {
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
	Size       int64               `json:"size"`
}

func (x *DescribeCollectionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&collDesc{
		Collection: x.Collection,
		Metadata:   x.Metadata,
		Schema:     x.Schema,
		Size:       x.Size,
	})
}

func (x *DescribeDatabaseResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Db          string            `json:"db"`
		Metadata    *DatabaseMetadata `json:"metadata"`
		Collections []*collDesc       `json:"collections"`
		Size        int64             `json:"size"`
	}{
		Db:       x.Db,
		Metadata: x.Metadata,
		Size:     x.Size,
	}

	for _, v := range x.Collections {
		resp.Collections = append(resp.Collections, &collDesc{
			Collection: v.Collection,
			Metadata:   v.Metadata,
			Schema:     v.Schema,
			Size:       v.Size,
		})
	}

	return json.Marshal(&resp)
}

func (x *EventsResponse) MarshalJSON() ([]byte, error) {
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

func (x *PublishRequest) UnmarshalJSON(data []byte) error {
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
		case "messages":
			var docs []jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &docs); err != nil {
				return err
			}

			x.Messages = make([][]byte, len(docs))
			for i := 0; i < len(docs); i++ {
				x.Messages[i] = docs[i]
			}
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}

			var options map[string]jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &options); err != nil {
				return err
			}

			part := false
			for oKey := range options {
				if oKey == "partition" {
					part = true
				}
			}
			if !part {
				// use -1 to indicate that no partition option was set
				x.Options.Partition = -1
			}
		}
	}
	return nil
}

func (x *SubscribeResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Message json.RawMessage `json:"message"`
	}{
		Message: x.Message,
	}
	return json.Marshal(resp)
}

// Proper marshal timestamp in metadata
type dmlResponse struct {
	Metadata      Metadata          `json:"metadata,omitempty"`
	Status        string            `json:"status,omitempty"`
	ModifiedCount int32             `json:"modified_count,omitempty"`
	Keys          []json.RawMessage `json:"keys,omitempty"`
}

func (x *InsertResponse) MarshalJSON() ([]byte, error) {
	var keys []json.RawMessage
	for _, k := range x.Keys {
		keys = append(keys, k)
	}
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status, Keys: keys})
}

func (x *ReplaceResponse) MarshalJSON() ([]byte, error) {
	var keys []json.RawMessage
	for _, k := range x.Keys {
		keys = append(keys, k)
	}
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status, Keys: keys})
}

func (x *DeleteResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status})
}

func (x *UpdateResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&dmlResponse{Metadata: CreateMDFromResponseMD(x.Metadata), Status: x.Status, ModifiedCount: x.ModifiedCount})
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

// Explicit custom marshalling of some search data structures required
// to retain schema in the output even when fields are empty.

func (x *SearchResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Hits   []*SearchHit            `json:"hits"`
		Facets map[string]*SearchFacet `json:"facets"`
		Meta   *SearchMetadata         `json:"meta"`
	}{
		Hits:   x.Hits,
		Facets: x.Facets,
		Meta:   x.Meta,
	}

	if resp.Hits == nil {
		resp.Hits = make([]*SearchHit, 0)
	}
	if resp.Facets == nil {
		resp.Facets = make(map[string]*SearchFacet)
	}
	return json.Marshal(resp)
}

func (x *SearchHit) MarshalJSON() ([]byte, error) {
	resp := struct {
		Data     json.RawMessage   `json:"data,omitempty"`
		Metadata SearchHitMetadata `json:"metadata,omitempty"`
	}{
		Data:     x.Data,
		Metadata: CreateMDFromSearchMD(x.Metadata),
	}
	return json.Marshal(resp)
}

func (x *SearchMetadata) MarshalJSON() ([]byte, error) {
	resp := struct {
		Found      int64 `json:"found"`
		TotalPages int32 `json:"total_pages"`
		Page       *Page `json:"page"`
	}{
		Found:      x.Found,
		TotalPages: x.TotalPages,
		Page:       x.Page,
	}
	return json.Marshal(resp)
}

func (x *SearchFacet) MarshalJSON() ([]byte, error) {
	resp := struct {
		Counts []*FacetCount `json:"counts"`
		Stats  *FacetStats   `json:"stats"`
	}{
		Counts: x.Counts,
		Stats:  x.Stats,
	}
	if resp.Counts == nil {
		resp.Counts = make([]*FacetCount, 0)
	}
	return json.Marshal(resp)
}

func (x *FacetStats) MarshalJSON() ([]byte, error) {
	resp := struct {
		Avg   *float64 `json:"avg,omitempty"`
		Max   *float64 `json:"max,omitempty"`
		Min   *float64 `json:"min,omitempty"`
		Sum   *float64 `json:"sum,omitempty"`
		Count int64    `json:"count"`
	}{
		Avg:   x.Avg,
		Max:   x.Max,
		Min:   x.Min,
		Sum:   x.Sum,
		Count: x.Count,
	}
	return json.Marshal(resp)
}

type SearchHitMetadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type Metadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func CreateMDFromResponseMD(x *ResponseMetadata) Metadata {
	var md Metadata
	if x == nil {
		return md
	}
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

func CreateMDFromSearchMD(x *SearchHitMeta) SearchHitMetadata {
	var md SearchHitMetadata
	if x == nil {
		return md
	}
	if x.CreatedAt != nil {
		tm := x.CreatedAt.AsTime()
		md.CreatedAt = &tm
	}
	if x.UpdatedAt != nil {
		tm := x.UpdatedAt.AsTime()
		md.UpdatedAt = &tm
	}

	return md
}

func unmarshalAdditionalFunction(data []byte) (*AdditionalFunction, error) {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	result := &AdditionalFunction{}
	for key, value := range mp {
		switch key {
		case "rollup":
			rollup, err := unmarshalRollup(value)
			if err != nil {
				return nil, err
			}
			result.Rollup = rollup
		}
	}
	return result, nil
}

func unmarshalRollup(data []byte) (*RollupFunction, error) {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return nil, err
	}

	result := &RollupFunction{}
	for key, value := range mp {
		switch key {
		case "aggregator":
			{
				var t string
				if err := jsoniter.Unmarshal(value, &t); err != nil {
					return nil, err
				}
				switch strings.ToUpper(t) {
				case "SUM":
					result.Aggregator = RollupAggregator_ROLLUP_AGGREGATOR_SUM
				case "COUNT":
					result.Aggregator = RollupAggregator_ROLLUP_AGGREGATOR_COUNT
				case "MIN":
					result.Aggregator = RollupAggregator_ROLLUP_AGGREGATOR_MIN
				case "MAX":
					result.Aggregator = RollupAggregator_ROLLUP_AGGREGATOR_MAX
				case "AVG":
					result.Aggregator = RollupAggregator_ROLLUP_AGGREGATOR_AVG

				}
			}
		case "interval":
			var t int64
			if err := jsoniter.Unmarshal(value, &t); err != nil {
				return nil, err
			}
			result.Interval = t
		}
	}
	return result, nil
}
