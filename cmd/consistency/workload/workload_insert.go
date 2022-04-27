package workload

import (
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
)

type InsertOnlyWorkload struct {
	Threads      int16
	Records      int64
	Database     string
	Collections  []string
	Schemas      [][]byte
	WorkloadData *Queue
}

func (w *InsertOnlyWorkload) Setup(client driver.Driver) error {
	w.WorkloadData = NewQueue(w.Collections)

	// cleanup first
	err := client.DropDatabase(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("dropped database failed, ignoring error '%s'", w.Database)
	}

	err = client.CreateDatabase(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("created database failed ignoring error '%s'", w.Database)
	}

	tx, err := client.BeginTx(context.TODO(), w.Database)
	if err != nil {
		return errors.Wrapf(err, "begin tx failed for db '%s'", w.Database)
	}

	for i := 0; i < len(w.Schemas); i++ {
		if err = tx.CreateOrUpdateCollection(context.TODO(), w.Collections[i], w.Schemas[i]); err != nil {
			return errors.Wrapf(err, "CreateOrUpdateCollection failed for db '%s' coll '%s'", w.Database, w.Collections[i])
		}
	}

	return tx.Commit(context.TODO())
}

func (w *InsertOnlyWorkload) Start(client driver.Driver) error {
	var insertErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		uniqueIdentifier := w.Records + int64(i)*w.Records
		go func(id int64) {
			defer wg.Done()
			for j := int64(0); j < w.Records; j++ {
				doc := NewDocument(id)
				serialized, err := Serialize(doc)
				if err != nil {
					insertErr = multierror.Append(insertErr, err)
					return
				}

				for k := 0; k < len(w.Collections); k++ {
					if _, err := client.Insert(context.TODO(), w.Database, w.Collections[k], []driver.Document{serialized}); err != nil {
						insertErr = multierror.Append(insertErr, errors.Wrapf(err, "insert to collection failed '%s' '%s'", w.Database, w.Collections[j]))
						return
					}

					w.WorkloadData.Insert(w.Collections[k], doc)
					log.Debug().Msgf("inserted document '%s' '%s' '%v'\n", w.Database, w.Collections[k], doc)
				}
				id++
			}
		}(uniqueIdentifier)
	}

	wg.Wait()

	return insertErr
}

func (w *InsertOnlyWorkload) Check(client driver.Driver) (bool, error) {
	isSuccess := false
	for _, collection := range w.Collections {
		it, err := client.Read(context.TODO(), w.Database, collection, driver.Filter(`{}`), nil)
		if err != nil {
			return false, errors.Wrapf(err, "read to collection failed '%s' '%s'", w.Database, collection)
		}

		queueDoc := NewQueueDocuments(collection)
		var doc driver.Document
		for it.Next(&doc) {
			document, err := Deserialize(doc)
			if err != nil {
				return false, errors.Wrapf(err, "deserialzing document failed")
			}
			log.Debug().Msgf("read document '%s' '%s' '%v'", w.Database, collection, document)
			queueDoc.Insert(document)
		}

		if it.Err() != nil {
			return false, it.Err()
		}

		existing := w.WorkloadData.Get(collection)
		if len(existing.Documents) != len(queueDoc.Documents) {
			log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks totalDocumentsInserted: %d, totalDocsRead: %d", w.Database, collection, len(existing.Documents), len(queueDoc.Documents))
		}

		sort.Slice(existing.Documents, func(i, j int) bool {
			return existing.Documents[i].F1 < existing.Documents[j].F1
		})
		sort.Slice(queueDoc.Documents, func(i, j int) bool {
			return queueDoc.Documents[i].F1 < queueDoc.Documents[j].F1
		})

		isSuccess = reflect.DeepEqual(existing.Documents, queueDoc.Documents)
		if !isSuccess {
			log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks", w.Database, collection)
		}
		return isSuccess, nil
	}

	return true, nil
}
