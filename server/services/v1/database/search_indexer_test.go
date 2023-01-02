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

package database

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	encoder "github.com/tigrisdata/tigris/lib/json"
	"github.com/tigrisdata/tigris/schema"
)

func TestFlattenObj(t *testing.T) {
	rawData := []byte(`{"a":1, "b": {"c": {"d": "foo", "c_nested": {"c_2": 10}}, "e": 3, "f": [1, 2, 3]}, "out": 10, "all_out": {"yes": 5}}`)
	var UnFlattenMap map[string]any
	require.NoError(t, jsoniter.Unmarshal(rawData, &UnFlattenMap))

	flattened := FlattenObjects(UnFlattenMap)
	require.Equal(t, "foo", flattened["b.c.d"])
	require.Equal(t, float64(3), flattened["b.e"])
	require.Equal(t, []interface{}{float64(1), float64(2), float64(3)}, flattened["b.f"])

	require.True(t, reflect.DeepEqual(UnFlattenMap, UnFlattenObjects(flattened)))
}

func TestPackSearchFields(t *testing.T) {
	nanoTs := internal.CreateNewTimestamp(int64(1641024000000000000)) // 2022-01-01
	emptyColl := &schema.DefaultCollection{}

	t.Run("with empty data should throw error", func(t *testing.T) {
		td := &internal.TableData{}
		res, err := PackSearchFields(td, emptyColl, "1")
		require.ErrorContains(t, err, "EOF")
		require.Nil(t, res)
	})

	mdTestCases := []struct {
		name    string
		rawData []byte
	}{
		{"metadata fields packed from table data", []byte(`{}`)},
		{"metadata fields in raw data are overridden", []byte(`{"created_at":123,"updated_at":542}`)},
	}

	for _, v := range mdTestCases {
		t.Run(v.name, func(t *testing.T) {
			td := &internal.TableData{
				CreatedAt: nanoTs,
				UpdatedAt: nanoTs,
				RawData:   v.rawData,
			}
			td.RawData = v.rawData
			res, err := PackSearchFields(td, emptyColl, "123")
			require.NoError(t, err)

			decData, err := encoder.Decode(res)
			require.NoError(t, err)

			createdAt, err := decData["created_at"].(json.Number).Int64()
			require.NoError(t, err)
			require.Equal(t, createdAt, td.CreatedAt.UnixNano())

			updatedAt, err := decData["updated_at"].(json.Number).Int64()
			require.NoError(t, err)
			require.Equal(t, updatedAt, td.UpdatedAt.UnixNano())
		})
	}

	t.Run("nil metadata fields are skipped", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{}`),
		}
		res, err := PackSearchFields(td, emptyColl, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)

		createdAt, err := decData["created_at"].(json.Number).Int64()
		require.NoError(t, err)
		require.Equal(t, createdAt, td.CreatedAt.UnixNano())

		updatedAt := decData["updated_at"]
		require.Nil(t, updatedAt)
	})

	t.Run("id in data is shadowed by internal key", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"id":"myData_321"}`),
		}
		res, err := PackSearchFields(td, emptyColl, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)

		require.Equal(t, decData["id"], "123")
		require.Equal(t, decData[schema.ReservedFields[schema.IdToSearchKey]], "myData_321")
	})

	t.Run("id not shadowed if not in document", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"some_id":"myData_321"}`),
		}
		res, err := PackSearchFields(td, emptyColl, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)

		require.Equal(t, decData["id"], "123")
		require.Equal(t, decData["some_id"], "myData_321")
		require.Nil(t, decData[schema.ReservedFields[schema.IdToSearchKey]])
	})

	t.Run("nested objects are flattened", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"parent":{"node_1":123,"node_2":"nested"}}`),
		}
		res, err := PackSearchFields(td, emptyColl, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)
		require.Equal(t, decData["parent.node_2"], "nested")

		v, err := strconv.Atoi(string(decData["parent.node_1"].(json.Number)))
		require.NoError(t, err)
		require.Equal(t, v, 123)
	})

	t.Run("array type of schema fields are unpacked", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"arrayField":[1,2,3,4,5]}`),
		}
		f := &schema.Field{DataType: schema.ArrayType, FieldName: "arrayField"}
		coll := &schema.DefaultCollection{
			QueryableFields: schema.BuildQueryableFields([]*schema.Field{f}, nil),
		}

		res, err := PackSearchFields(td, coll, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)
		require.Equal(t, "[1,2,3,4,5]", decData["arrayField"])
	})

	t.Run("dateTime type of schema fields are unpacked", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"dateField":"2022-10-11T04:19:32+05:30"}`),
		}
		f := &schema.Field{DataType: schema.DateTimeType, FieldName: "dateField"}
		coll := &schema.DefaultCollection{
			QueryableFields: schema.BuildQueryableFields([]*schema.Field{f}, nil),
		}
		res, err := PackSearchFields(td, coll, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)

		require.Equal(t, "2022-10-11T04:19:32+05:30", decData[schema.ToSearchDateKey(f.Name())])
		d, err := decData["dateField"].(json.Number).Int64()
		require.NoError(t, err)
		require.Equal(t, int64(1665442172000000000), d)
	})

	t.Run("values are encoded to their types", func(t *testing.T) {
		td := &internal.TableData{
			CreatedAt: nanoTs,
			RawData:   []byte(`{"strField":"strValue", "floatField":99999.12345, "intField": 12, "nilField": null}`),
		}
		res, err := PackSearchFields(td, emptyColl, "123")
		require.NoError(t, err)

		decData, err := encoder.Decode(res)
		require.NoError(t, err)
		require.Equal(t, "strValue", decData["strField"])
		require.Nil(t, decData["nilField"])

		v, err := decData["intField"].(json.Number).Int64()
		require.NoError(t, err)
		require.Equal(t, int64(12), v)

		f, err := decData["floatField"].(json.Number).Float64()
		require.NoError(t, err)
		require.Equal(t, 99999.12345, f)
	})
}

func TestUnpackSearchFields(t *testing.T) {
	emptyColl := &schema.DefaultCollection{}

	t.Run("no id key in schema, extract search key", func(t *testing.T) {
		doc := map[string]any{
			"id": "123",
		}

		searchKey, _, unpacked, err := UnpackSearchFields(doc, emptyColl)
		require.NoError(t, err)
		require.Equal(t, doc["id"], searchKey)
		require.Len(t, unpacked, 0)
	})

	t.Run("populate id key and search key", func(t *testing.T) {
		doc := map[string]any{
			"id":         "internalId",
			"_tigris_id": 123,
		}
		searchKey, _, unpacked, err := UnpackSearchFields(doc, emptyColl)
		require.NoError(t, err)
		require.Len(t, unpacked, 1)
		require.Equal(t, doc[schema.ReservedFields[schema.IdToSearchKey]], unpacked["id"])
		require.Equal(t, doc["id"], searchKey)
	})

	t.Run("created_at metadata gets populated", func(t *testing.T) {
		doc := map[string]any{
			"id":         "123",
			"created_at": json.Number("1666054267528106000"),
		}
		_, td, unpacked, err := UnpackSearchFields(doc, emptyColl)
		require.NoError(t, err)
		require.Empty(t, unpacked)
		expected, _ := doc["created_at"].(json.Number).Int64()
		require.Equal(t, expected, td.CreatedAt.UnixNano())
		require.Nil(t, td.UpdatedAt)
	})

	t.Run("updated_at metadata gets populated", func(t *testing.T) {
		doc := map[string]any{
			"id":         "123",
			"updated_at": json.Number("1666054267528106000"),
		}
		_, td, unpacked, err := UnpackSearchFields(doc, emptyColl)
		require.NoError(t, err)
		require.Empty(t, unpacked)
		expected, _ := doc["updated_at"].(json.Number).Int64()
		require.Equal(t, expected, td.UpdatedAt.UnixNano())
		require.Nil(t, td.CreatedAt)
	})

	t.Run("nested object is unflattened", func(t *testing.T) {
		doc := map[string]any{
			"id":            "123",
			"parent.node_1": 123,
			"parent.node_2": "someData",
		}
		_, _, unpacked, err := UnpackSearchFields(doc, emptyColl)
		require.NoError(t, err)
		require.Len(t, unpacked, 1)
		require.Len(t, unpacked["parent"], 2)
		require.Equal(t, map[string]interface{}{"node_1": 123, "node_2": "someData"}, unpacked["parent"])
	})

	t.Run("array type is unpacked from string", func(t *testing.T) {
		doc := map[string]any{
			"id":         "123",
			"arrayField": "[1.1,2.1,3.0,4.3,5.5]",
		}
		f := &schema.Field{DataType: schema.ArrayType, FieldName: "arrayField"}
		coll := &schema.DefaultCollection{
			QueryableFields: schema.BuildQueryableFields([]*schema.Field{f}, nil),
		}
		_, _, unpacked, err := UnpackSearchFields(doc, coll)
		require.NoError(t, err)
		require.Len(t, unpacked, 1)
		require.Equal(t, []interface{}{1.1, 2.1, 3.0, 4.3, 5.5}, unpacked["arrayField"])
	})

	t.Run("array type not packed as string", func(t *testing.T) {
		doc := map[string]any{
			"id":         "123",
			"arrayField": []interface{}{1.1, 2.1, 3.0, 4.3, 5.5},
		}
		f := &schema.Field{DataType: schema.ArrayType, FieldName: "arrayField"}
		coll := &schema.DefaultCollection{
			QueryableFields: schema.BuildQueryableFields([]*schema.Field{f}, nil),
		}
		_, _, unpacked, err := UnpackSearchFields(doc, coll)
		require.NoError(t, err)
		require.Len(t, unpacked, 1)
		require.Equal(t, []interface{}{1.1, 2.1, 3.0, 4.3, 5.5}, unpacked["arrayField"])
	})

	t.Run("dateTime fields are unpacked", func(t *testing.T) {
		doc := map[string]any{
			"id":                     "123",
			"dateField":              1665442172000000000,
			"_tigris_date_dateField": "2022-10-11T04:19:32+05:30",
		}
		f := &schema.Field{DataType: schema.DateTimeType, FieldName: "dateField"}
		coll := &schema.DefaultCollection{
			QueryableFields: schema.BuildQueryableFields([]*schema.Field{f}, nil),
		}
		_, _, unpacked, err := UnpackSearchFields(doc, coll)
		require.NoError(t, err)
		require.Len(t, unpacked, 1)
		require.Equal(t, "2022-10-11T04:19:32+05:30", unpacked["dateField"])
	})
}

// Benchmarking to test if it makes sense to decode the data and then add fields to the decoded map and then encode
// again and this benchmark shows if we are setting more than one field then it is better to decode.
func BenchmarkEncDec(b *testing.B) {
	js := []byte(`{
	"name": "Women's Fiona Handbag",
	"brand": "Michael Cors",
	"labels": "Handbag, Purse, Women's fashion",
	"price": 99999.12345,
	"key": "1",
	"categories": ["random", "fashion", "handbags", "women's"],
	"description": "A typical product catalog will have many json objects like this. This benchmark is testing if decoding/encoding is better than mutating JSON directly.",
	"random": "abc defg hij klm nopqr stuv wxyz 1234 56 78 90 abcd efghijkl mnopqrstuvwxyzA BCD EFGHIJKL MNOPQRS TUVW XYZ"
}`)

	id := "1"
	for i := 0; i < b.N; i++ {
		var data map[string]interface{}
		if err := jsoniter.Unmarshal(js, &data); err != nil {
			require.NoError(b, err)
		}

		data[schema.SearchId] = id
		data["created_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		data["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		_, err := jsoniter.Marshal(data)
		require.NoError(b, err)
	}
}

func BenchmarkJSONSet(b *testing.B) {
	js := []byte(`{
	"name": "Women's Fiona Handbag",
	"brand": "Michael Cors",
	"labels": "Handbag, Purse, Women's fashion",
	"price": 99999.12345,
	"key": "1",
	"categories": ["random", "fashion", "handbags", "women's"],
	"description": "A typical product catalog will have many json objects like this. This benchmark is testing if decoding/encoding is better than mutating JSON directly.",
	"random": "abc defg hij klm nopqr stuv wxyz 1234 56 78 90 abcd efghijkl mnopqrstuvwxyzA BCD EFGHIJKL MNOPQRS TUVW XYZ"
}`)

	var err error
	id := "1"
	for i := 0; i < b.N; i++ {
		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, id)), schema.SearchId)
		require.NoError(b, err)

		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, "created_at")), time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(b, err)

		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, "updated_at")), time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(b, err)
	}
}
