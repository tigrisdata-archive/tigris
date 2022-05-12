package main

import (
	"context"
	"fmt"
	"math/rand"
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

func collectionsForLoadTest() ([]string, [][]byte) {
	// first is integer primary key, second is string primary key
	return []string{"c1", "c2"}, [][]byte{
		[]byte(`{
	"title": "c1",
	"properties": {
		"F1": {
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
	"primary_key": ["F1"]
}`),
		[]byte(`{
	"title": "c2",
	"properties": {
		"F1": {
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

func CreateWorkloads() []Workload {
	collections, schemas := collectionsForLoadTest()
	var workload []Workload
	workload = append(workload, &workload2.DDLWorkload{
		Threads:     1,
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
		Records:     32,
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
	rand.Seed(time.Now().Unix())

	driver.DefaultProtocol = driver.HTTP
	client, err := driver.NewDriver(context.TODO(), &clientConfig.Driver{
		URL: fmt.Sprintf("http://%s:%d", "localhost", 8081),
	})
	if err != nil {
		panic(err)
	}

	workloads := CreateWorkloads()
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
