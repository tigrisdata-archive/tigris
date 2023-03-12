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

package search

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util"
)

func TestMutateSearchDocument(t *testing.T) {
	idx := []byte(`{
	"title": "t1",
	"properties": {
		"a": {
			"type": "integer"
		},
		"b": {
			"type": "string"
		},
		"c": {
			"type": "object",
			"properties": {
				"e": {
					"type": "integer"
				},
				"f": {
					"type": "string"
				},
				"g": {
					"type": "object",
					"properties": {
						"h": {
							"type": "string"
						},
						"i": {
                            "searchIndex": false,
							"type": "object",
						    "properties": {}
						}
					}
				}
			}
		},
		"d": {
			"type": "object",
			"properties": {}
		}
	}
}`)

	factory, err := schema.NewFactoryBuilder(true).BuildSearch("t1", idx)
	require.NoError(t, err)

	index := schema.NewSearchIndex(1, "test", factory, nil)

	mp, err := util.JSONToMap([]byte(`{
	"a": 1,
	"b": 2.0,
	"c": {
		"e": 1,
		"f": 2.0,
		"g": {
			"h": "FOO",
			"i": {
				"i_nest": "index i nested"
			}
		}
	},
	"d": {
		"agent": "java client",
		"message": "alright"
	}
}`))
	require.NoError(t, err)

	_, err = MutateSearchDocument(index, internal.NewTimestamp(), mp, false)
	require.NoError(t, err)
}
