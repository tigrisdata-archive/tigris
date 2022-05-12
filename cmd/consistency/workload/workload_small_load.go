package workload

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
	"sync"
)

func randomCollection(collectionName string) (string, []byte) {
	// first is integer primary key, second is string primary key
	return collectionName, []byte(fmt.Sprintf(`{
	"title": "%s",
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
}`, collectionName))
}

type SmallConciseWorkload struct {
	Threads      int16
	Records      int64
	Database     string
	Collections  []string
	Schemas      [][]byte
	WorkloadData *Queue
}

func (w *SmallConciseWorkload) Type() string {
	return "small_only_workload"
}

func (w *SmallConciseWorkload) Setup(client driver.Driver) error {
	return nil
}

func (w *SmallConciseWorkload) Start1(client driver.Driver) (int64, error) {
	var replaceErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)

		uniqueIdentifier := w.Records + int64(i)*w.Records
		go func(id int64) {
			defer wg.Done()

			randomDatabaseName := fmt.Sprintf("%s_%d", w.Database, id)
			_ = client.DropDatabase(context.TODO(), randomDatabaseName)

			err := client.CreateDatabase(context.TODO(), randomDatabaseName)
			if err != nil {
				log.Err(err).Msgf("created database failed ignoring error '%s'", randomDatabaseName)
			}

			db := client.UseDatabase(randomDatabaseName)
			err = db.CreateOrUpdateCollection(context.TODO(), w.Collections[0], w.Schemas[0])
			if err != nil {
				replaceErr = multierror.Append(replaceErr, err)
				return
			}

			for j := int64(0); j < w.Records; j++ {
				doc := NewDocument(id)
				serialized, err := Serialize(doc)
				if err != nil {
					replaceErr = multierror.Append(replaceErr, err)
					return
				}

				if _, err := client.UseDatabase(w.Database).Replace(context.TODO(), w.Collections[0], []driver.Document{serialized}); err != nil {
					replaceErr = multierror.Append(replaceErr, errors.Wrapf(err, "replace to collection failed '%s' '%s'", w.Database, w.Collections[j]))
					return
				}

				w.WorkloadData.Add(w.Collections[0], doc)
				id++
			}
		}(uniqueIdentifier)
	}

	wg.Wait()

	return w.Records * int64(w.Threads), replaceErr
}

func (w *SmallConciseWorkload) Start(client driver.Driver) (int64, error) {
	_ = client.DropDatabase(context.TODO(), w.Database)

	err := client.CreateDatabase(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("created database failed ignoring error '%s'", w.Database)
	}

	db := client.UseDatabase(w.Database)

	var replaceErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)

		uniqueIdentifier := w.Records + int64(i)*w.Records
		go func(id int64) {
			defer wg.Done()

			collection, schema := randomCollection(fmt.Sprintf("%s_%d", w.Collections[0], id))
			err = db.CreateOrUpdateCollection(context.TODO(), collection, schema)
			if err != nil {
				replaceErr = multierror.Append(replaceErr, err)
				log.Debug().Err(err).Msg("Create collection failed")
				return
			}

			for j := int64(0); j < w.Records; j++ {
				doc := NewDocument(id)
				serialized, err := Serialize(doc)
				if err != nil {
					replaceErr = multierror.Append(replaceErr, err)
					return
				}

				if _, err := db.Replace(context.TODO(), collection, []driver.Document{serialized}); err != nil {
					replaceErr = multierror.Append(replaceErr, errors.Wrapf(err, "replace to collection failed '%s' '%s'", w.Database, collection))
					log.Debug().Err(err).Msg("Replace document failed")
					return
				}
				id++
			}
		}(uniqueIdentifier)
	}

	wg.Wait()

	return w.Records * int64(w.Threads), replaceErr
}

func (w *SmallConciseWorkload) Check(client driver.Driver) (bool, error) {
	return true, nil
}
