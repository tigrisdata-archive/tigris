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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/container"
)

func TestUnFlatMap(t *testing.T) {
	t.Run("simple_flattened", func(t *testing.T) {
		input := make(map[string]any)
		input["app_metadata"] = nil
		input["app_metadata.provider"] = "foo"
		output := UnFlatMap(input, false)

		require.Equal(t, 1, len(output))

		expected := make(map[string]any)
		expected["provider"] = "foo"
		require.Equal(t, expected, output["app_metadata"])
	})
	t.Run("nested_map", func(t *testing.T) {
		flattened := []byte(`{
	"address.country": "USA",
	"address.postalCode": "12345",
	"arr_obj_1": [{
		"createdAt": "2023-05-06T16:30:49.54288Z"
	}, {
		"createdAt": "2023-05-06T17:46:29.79824Z"
	}],
	"arr_prim": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday"],
	"obj.description": "test description",
	"obj.name": "toplevel",
    "obj.arr": [{
		"createdAt": "2023-05-06T16:30:49.54288Z"
	}, {
		"createdAt": "2023-05-06T17:46:29.79824Z"
	}]
}`)

		mp, err := JSONToMap(flattened)
		require.NoError(t, err)
		mp = FlatMap(mp, container.NewHashSet())
		unFlat := UnFlatMap(mp, false)
		require.Equal(t, unFlat["arr_obj_1"].([]any)[0].(map[string]any)["createdAt"], "2023-05-06T16:30:49.54288Z")
	})
}
