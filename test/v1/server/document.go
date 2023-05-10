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

//go:build integration

package server

import (
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

type (
	Map map[string]any
	Doc Map
)

var FakeCollectionSchema = []byte(`{
  "schema": {
    "title": "fake_collection",
    "primary_key": ["id"],
    "properties": {
      "id": {
        "type": "string"
      },
      "name": {
        "type": "string"
      },
      "sentence": {
        "type": "string"
      },
      "quote": {
        "type": "string"
      },
      "phrase": {
        "type": "string"
      },
      "paragraph1": {
        "type": "string"
      },
      "paragraph2": {
        "type": "string"
      },
      "paragraph3": {
        "type": "string"
      },
      "paragraph": {
        "type": "string"
      },
      "random": {
        "type": "string"
      },
      "number": {
        "type": "number"
      },
      "url": {
        "type": "string"
      },
      "placeholder": {
        "searchIndex": true,
        "sort": true,
        "type": "string"
      },
      "created_at": {
        "type": "string",
        "format": "date-time"
      },
      "updated_at": {
        "type": "string",
        "format": "date-time"
      },
      "map": {
        "type": "object",
        "properties": {}
      },
      "cars": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "animals": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "fruits": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "nested": {
        "type": "object",
        "properties": {
          "timestamp": {
            "type": "integer"
          },
          "paragraph": {
            "type": "string"
          },
          "places": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "vegetables": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "dessert": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "object": {
            "searchIndex": false,
            "type": "object",
            "properties": {}
          },
          "address": {
            "type": "object",
            "properties": {
              "city": {
                "type": "string"
              },
              "state": {
                "type": "string"
              },
              "countryName": {
                "type": "string"
              },
              "latitude": {
                "type": "array",
                "items": {
                  "type": "number"
                }
              },
              "longitude": {
                "type": "array",
                "items": {
                  "type": "number"
                }
              }
            }
          }
        }
      }
    }
  }
}`)

var FakeDocumentSchema = []byte(`{
  "schema": {
    "title": "fake_index",
    "properties": {
      "name": {
        "type": "string"
      },
      "sentence": {
        "type": "string"
      },
      "quote": {
        "type": "string"
      },
      "phrase": {
        "type": "string"
      },
      "paragraph1": {
        "type": "string"
      },
      "paragraph2": {
        "type": "string"
      },
      "paragraph3": {
        "type": "string"
      },
      "paragraph": {
        "type": "string"
      },
      "random": {
        "type": "string"
      },
      "number": {
        "type": "number"
      },
      "url": {
        "type": "string"
      },
      "placeholder": {
        "type": "string"
      },
      "created_at": {
        "type": "string",
        "format": "date-time"
      },
      "updated_at": {
        "type": "string",
        "format": "date-time"
      },
      "map": {
        "searchIndex": false,
        "type": "object",
        "properties": {}
      },
      "cars": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "animals": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "fruits": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "nested": {
        "type": "object",
        "properties": {
          "timestamp": {
            "type": "integer"
          },
          "paragraph": {
            "type": "string"
          },
          "places": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "vegetables": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "dessert": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "object": {
            "searchIndex": false,
            "type": "object",
            "properties": {}
          },
          "address": {
            "type": "object",
            "properties": {
              "city": {
                "type": "string"
              },
              "state": {
                "type": "string"
              },
              "country_name": {
                "type": "string"
              },
              "latitude": {
                "type": "array",
                "items": {
                  "type": "number"
                }
              },
              "longitude": {
                "type": "array",
                "items": {
                  "type": "number"
                }
              }
            }
          }
        }
      }
    }
  }
}`)

type FakeDocument struct {
	Id         string  `json:"id"`
	Name       string  `json:"name" fake:"{firstname}"`
	Sentence   string  `json:"sentence" fake:"{sentence:50}"`
	Quote      string  `json:"quote" fake:"{quote}"`
	Phrase     string  `json:"phrase" fake:"{phrase}"`
	Paragraph  string  `json:"paragraph" fake:"{paragraph:10,10,50}"`
	Paragraph1 string  `json:"paragraph1" fake:"{paragraph:10,10,50}"`
	Paragraph2 string  `json:"paragraph2" fake:"{paragraph:10,10,50}"`
	Paragraph3 string  `json:"paragraph3" fake:"{paragraph:10,10,50}"`
	Random     string  `json:"random" fake:"{nouncollectivething}"`
	Number     float64 `json:"number" fake:"{number:1,10000}"`
	URL        string  `json:"url" fake:"{url}"`

	Cars        []string  `json:"cars" fake:"{carmaker}" fakesize:"10000"`
	Animals     []string  `json:"animals" fake:"{animal}" fakesize:"10000"`
	Fruits      []string  `json:"fruits" fake:"{fruit}" fakesize:"10000"`
	Placeholder string    `json:"placeholder"`
	Nested      Nested    `json:"nested"`
	CreatedAt   time.Time `json:"created_at"  fake:"{date}"`
	UpdatedAt   time.Time `json:"updated_at"  fake:"{date}"`
}

type Nested struct {
	Timestamp  int64    `json:"timestamp" fake:"{nanosecond}"`
	Paragraph  string   `json:"paragraph"  fake:"{paragraph:10,10,50}"`
	Address    Address  `json:"address"`
	Places     []string `json:"places"  fake:"{city}" fakesize:"10000"`
	Vegetables []string `json:"vegetables"  fake:"{vegetable}" fakesize:"10000"`
	Dessert    []string `json:"dessert"  fake:"{dessert}" fakesize:"10000"`
}

type Address struct {
	City        string    `json:"city"  fake:"{city}"`
	State       string    `json:"state"  fake:"{state}"`
	CountryName string    `json:"countryName"  fake:"{country}"`
	Latitude    []float64 `json:"latitude"  fake:"{latitude}" fakesize:"10000"`
	Longitude   []float64 `json:"longitude"  fake:"{longitude}" fakesize:"10000"`
}

// NewFakeDocument generates a ~3MB fake document by default.
func NewFakeDocument(id string, placeholder string) FakeDocument {
	var address Address
	_ = gofakeit.Struct(&address)

	var nested Nested
	_ = gofakeit.Struct(&nested)
	nested.Address = address

	var f FakeDocument
	_ = gofakeit.Struct(&f)
	f.Nested = nested
	f.Placeholder = placeholder
	f.Id = id

	return f
}

func Serialize(f FakeDocument) ([]byte, error) {
	return jsoniter.Marshal(f)
}

func GenerateFakes(t *testing.T, ids []string, placeholder []string) ([]FakeDocument, [][]byte) {
	var fakes []FakeDocument
	var serialized [][]byte
	for i := 0; i < len(ids); i++ {
		fake := NewFakeDocument(ids[i], placeholder[i])
		payload, err := Serialize(fake)
		require.NoError(t, err)

		fakes = append(fakes, fake)
		serialized = append(serialized, payload)
	}

	return fakes, serialized
}

func GenerateFakesForDoc(t *testing.T, ids []string) ([]FakeDocument, []Doc) {
	return GenerateFakesForDocWithPlaceholder(t, ids, nil)
}

func GenerateFakesForDocWithPlaceholder(t *testing.T, ids []string, placeholder []string) ([]FakeDocument, []Doc) {
	if len(placeholder) == 0 {
		placeholder = make([]string, len(ids))
	}

	var fakes []FakeDocument
	var documents []Doc
	for i := 0; i < len(ids); i++ {
		fake := NewFakeDocument(ids[i], placeholder[i])
		payload, err := Serialize(fake)
		require.NoError(t, err)

		fakes = append(fakes, fake)

		var doc Doc
		require.NoError(t, jsoniter.Unmarshal(payload, &doc))
		documents = append(documents, doc)
	}

	return fakes, documents
}

func GenerateDocFromFake(t *testing.T, f FakeDocument) Doc {
	payload, err := Serialize(f)
	require.NoError(t, err)

	var doc Doc
	require.NoError(t, jsoniter.Unmarshal(payload, &doc))
	return doc
}
