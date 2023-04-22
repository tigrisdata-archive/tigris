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

package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	clientConfig "github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	workload2 "github.com/tigrisdata/tigris/cmd/consistency/workload"
)

type Workload interface {
	Setup(client driver.Driver) error
	Start(client driver.Driver) (int64, error)
	Check(client driver.Driver) (bool, error)
	Type() string
}

func indexesForLoadTest() (string, []byte) {
	return "t1", []byte(`{
  "title": "t1",
  "properties": {
    "cars": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "created_at": {
      "type": "string",
      "format": "date-time",
      "sort": true
    },
    "id": {
      "type": "integer"
    },
    "updated_at": {
      "type": "string",
      "format": "date-time",
      "sort": true
    },
    "nested": {
      "type": "object",
      "properties": {
        "random": {
          "type": "string",
          "facet": true
        },
        "nested_id": {
          "type": "string",
          "format": "uuid",
          "id": true
        },
        "address": {
          "type": "object",
          "properties": {
            "city": {
              "type": "string",
              "facet": true
            },
            "state": {
              "type": "string",
              "facet": true
            },
            "country": {
              "type": "string",
              "facet": true
            }
          }
        },
        "name": {
          "type": "string",
          "facet": true
        },
        "url": {
          "type": "string",
          "facet": true
        },
        "domain": {
          "type": "string"
        },
        "labels": {
          "type": "array",
          "items": {
        	"type": "string"
          }
        },
        "company": {
          "type": "string",
          "facet": true
        },
        "timestamp": {
          "type": "integer",
          "sort": true
        }
      }
    }
  }
}`)
}

func collectionsForBigPayloadLoadTest() (string, []byte) {
	return "compression_c1", []byte(`{
  "title": "compression_c1",
  "primary_key": ["pkey"],
  "properties": {
    "cars": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "food": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "created_at": {
      "type": "string",
      "format": "date-time"
    },
    "id": {
      "type": "integer"
    },
    "pkey": {
      "type": "string",
      "autoGenerate": true
    },
    "updated_at": {
      "type": "string",
      "format": "date-time"
    },
    "nested": {
      "type": "object",
      "properties": {
        "random": {
          "type": "string"
        },
        "nested_id": {
          "type": "string",
          "format": "uuid"
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
            "country": {
              "type": "string"
            }
          }
        },
        "name": {
          "type": "string"
        },
        "sentence": {
          "type": "string"
        },
        "url": {
          "type": "string"
        },
        "domain": {
          "type": "string"
        },
        "labels": {
          "type": "array",
          "items": {
        	"type": "string"
          }
        },
        "company": {
          "type": "string"
        },
        "timestamp": {
          "type": "integer"
        }
      }
    }
  }
}`)
}

func collectionsForLoadTest() ([]string, [][]byte) {
	// first is integer primary key, second is string primary key
	return []string{"c1", "c2"}, [][]byte{
		[]byte(`{
	"title": "c1",
	"properties": {
		"id": {
			"type": "integer"
		},
		"F2": {
			"type": "string"
		},
		"F3": {
			"type": "string",
			"format": "byte"
		},
		"F4": {
			"type": "string",
			"format": "uuid"
		},
		"F5": {
			"type": "string",
			"format": "date-time"
		}
	},
	"primary_key": ["id"]
}`),
		[]byte(`{
	"title": "c2",
	"properties": {
		"id": {
			"type": "integer"
		},
		"F2": {
			"type": "string"
		},
		"F3": {
			"type": "string",
			"format": "byte"
		},
		"F4": {
			"type": "string",
			"format": "uuid"
		},
		"F5": {
			"type": "string",
			"format": "date-time"
		}
	},
	"primary_key": ["F2"]
}`),
	}
}

func CreateSearchWorkload() []Workload {
	index, schema := indexesForLoadTest()
	var workload []Workload
	workload = append(workload, &workload2.SearchOnlyWorkload{
		Threads: 16,
		Records: 256,
		Project: "test1",
		Index:   index,
		Schema:  schema,
	})

	return workload
}

func CreateBigPayloadWorkload() []Workload {
	collections, schemas := collectionsForBigPayloadLoadTest()
	var workload []Workload
	workload = append(workload, &workload2.BigPayloadWorkload{
		Threads:     16,
		Records:     64,
		Database:    "test1",
		Collections: collections,
		// first is integer primary key, second is string primary key
		Schemas: schemas,
	})

	return workload
}

func CreateWorkloads() []Workload {
	collections, schemas := collectionsForLoadTest()
	var workload []Workload
	workload = append(workload, &workload2.DropCreateWriteWorkload{
		Threads:     8,
		Records:     1024,
		Database:    "test1",
		Collections: collections,
		// first is integer primary key, second is string primary key
		Schemas: schemas,
	})

	workload = append(workload, &workload2.DDLWorkload{
		Threads:     2,
		Database:    "test1",
		Collections: []string{collections[0]},
		Schemas:     [][]byte{schemas[0]},
	})

	workload = append(workload, &workload2.InsertOnlyWorkload{
		Threads:     64,
		Records:     64,
		Database:    "test1",
		Collections: collections,
		// first is integer primary key, second is string primary key
		Schemas: schemas,
	})

	workload = append(workload, &workload2.ReplaceOnlyWorkload{
		Threads:     32,
		Records:     64,
		Database:    "test1",
		Collections: collections,
		// first is integer primary key, second is string primary key
		Schemas: schemas,
	})

	workload = append(workload, &workload2.SmallConciseWorkload{
		Threads:     16,
		Records:     10,
		Database:    "test1",
		Collections: collections,
		// first is integer primary key, second is string primary key
		Schemas: schemas,
	})

	return workload
}

func main() {
	driver.DefaultProtocol = driver.HTTP
	client, err := driver.NewDriver(context.TODO(), &clientConfig.Driver{
		URL: fmt.Sprintf("http://%s", net.JoinHostPort("localhost", "8081")),
	})
	if err != nil {
		panic(err)
	}

	workloads := CreateWorkloads()
	workloads = append(workloads, CreateBigPayloadWorkload()...)
	workloads = append(workloads, CreateSearchWorkload()...)
	for _, w := range workloads {
		log.Debug().Msgf("running workload type %s", w.Type())
		if err = w.Setup(client); err != nil {
			log.Panic().Err(err).Msg("workload setup failed")
		}

		start := time.Now()
		records, err := w.Start(client)
		if err != nil {
			log.Panic().Err(err).Msg("workload start failed")
		}
		log.Debug().Msgf("load generated in %v, total records %d", time.Since(start), records)

		var success bool
		success, err = w.Check(client)
		if err != nil {
			log.Panic().Err(err).Msg("workload check failed")
		}
		if !success {
			log.Panic().Msgf("workload consistency issue, stopping %s", w.Type())
		} else {
			log.Debug().Msgf("workload is consistent %s", w.Type())
		}
	}
}
