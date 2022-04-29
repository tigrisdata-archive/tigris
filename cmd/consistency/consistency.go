package main

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	clientConfig "github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	workload2 "github.com/tigrisdata/tigris/cmd/consistency/workload"
)

type Workload interface {
	Setup(client driver.Driver) error
	Start(client driver.Driver) error
	Check(client driver.Driver) (bool, error)
	Type() string
}

func CreateWorkloads() []Workload {
	var workload []Workload
	workload = append(workload, &workload2.DDLWorkload{
		Threads:     16,
		Database:    "test1",
		Collections: []string{"c1"},
		Schemas: [][]byte{
			[]byte(`{
	"title": "c1",
	"properties": {
		"F1": {
			"type": "integer"
		},
		"F2": {
			"type": "string"
		}
	},
	"primary_key": ["F1"]
}`)},
	})

	workload = append(workload, &workload2.InsertOnlyWorkload{
		Threads:     64,
		Records:     64,
		Database:    "test1",
		Collections: []string{"c1", "c2"},
		// first is integer primary key, second is string primary key
		Schemas: [][]byte{
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
		},
	})

	return workload
}

func main() {
	driver.DefaultProtocol = driver.HTTP
	client, err := driver.NewDriver(context.TODO(), &clientConfig.Config{
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

		if err = w.Start(client); err != nil {
			log.Panic().Err(err).Msg("workload start failed")
		}
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
