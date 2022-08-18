// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
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

	var id = "1"
	for i := 0; i < b.N; i++ {
		var data map[string]interface{}
		if err := jsoniter.Unmarshal(js, &data); err != nil {
			require.NoError(b, err)
		}

		data[searchID] = id
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
	var id = "1"
	for i := 0; i < b.N; i++ {
		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, id)), searchID)
		require.NoError(b, err)

		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, "created_at")), time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(b, err)

		js, err = jsonparser.Set(js, []byte(fmt.Sprintf(`"%s"`, "updated_at")), time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(b, err)
	}
}
