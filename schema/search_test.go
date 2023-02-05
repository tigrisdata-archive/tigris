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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchIndex_CollectionSchema(t *testing.T) {
	reqSchema := []byte(`{
	"title": "t1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"id_32": {
			"type": "integer",
			"format": "int32"
		},
		"product": {
			"type": "string",
			"maxLength": 100
		},
		"id_uuid": {
			"type": "string",
			"format": "uuid"
		},
		"ts": {
			"type": "string",
			"format": "date-time"
		},
		"price": {
			"type": "number"
		},
		"simple_items": {
			"type": "array",
			"items": {
				"type": "integer"
			}
		},
		"simple_object": {
			"type": "object",
			"properties": {
				"name": {
					"type": "string"
				},
				"phone": {
					"type": "string"
				},
				"address": {
					"type": "object",
					"properties": {
						"street": {
							"type": "string"
						}
					}
				},
				"details": {
					"type": "object",
					"properties": {
						"nested_id": {
							"type": "integer"
						},
						"nested_obj": {
							"type": "object",
							"properties": {
								"id": {
									"type": "integer"
								},
								"name": {
									"type": "string"
								}
							}
						},
						"nested_array": {
							"type": "array",
							"items": {
								"type": "integer"
							}
						},
						"nested_string": {
							"type": "string"
						}
					}
				}
			}
		}
	},
	"primary_key": ["id"]
}`)

	schFactory, err := Build("t1", reqSchema)
	require.NoError(t, err)

	expFlattenedFields := []string{
		"id", "_tigris_id", "id_32", "product", "id_uuid", "ts", ToSearchDateKey("ts"), "price", "simple_items", "simple_object.name",
		"simple_object.phone", "simple_object.address.street", "simple_object.details.nested_id", "simple_object.details.nested_obj.id",
		"simple_object.details.nested_obj.name", "simple_object.details.nested_array", "simple_object.details.nested_string",
		"_tigris_created_at", "_tigris_updated_at",
	}

	implicitSearchIndex := NewImplicitSearchIndex("t1", "t1", schFactory.Fields, nil)
	for i, f := range implicitSearchIndex.StoreSchema.Fields {
		require.Equal(t, expFlattenedFields[i], f.Name)
	}
}
