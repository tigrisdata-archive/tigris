package workload

import (
	"context"
	"reflect"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
)

type ReplaceOnlyWorkload struct {
	Threads      int16
	Records      int64
	Database     string
	Collections  []string
	Schemas      [][]byte
	WorkloadData *Queue
}

func (w *ReplaceOnlyWorkload) Type() string {
	return "replace_only_workload"
}

func (w *ReplaceOnlyWorkload) Setup(client driver.Driver) error {
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

func (w *ReplaceOnlyWorkload) Start(client driver.Driver) (int64, error) {
	var replaceErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		uniqueIdentifier := w.Records + int64(i)*w.Records
		go func(id int64) {
			defer wg.Done()
			for j := int64(0); j < w.Records; j++ {
				doc := NewDocument(1)
				serialized, err := Serialize(doc)
				if err != nil {
					replaceErr = multierror.Append(replaceErr, err)
					return
				}

				for k := 0; k < len(w.Collections); k++ {
					if _, err := client.UseDatabase(w.Database).Replace(context.TODO(), w.Collections[k], []driver.Document{serialized}); err != nil {
						replaceErr = multierror.Append(replaceErr, errors.Wrapf(err, "insert to collection failed '%s' '%s'", w.Database, w.Collections[k]))
						return
					}

					w.WorkloadData.Add(w.Collections[k], doc)
					//log.Debug().Msgf("replaced document '%s' '%s' '%v'\n", w.Database, w.Collections[k], doc)
				}
				id++
			}
		}(uniqueIdentifier)
	}

	wg.Wait()

	return w.Records * int64(w.Threads), replaceErr
}

func (w *ReplaceOnlyWorkload) Check(client driver.Driver) (bool, error) {
	isSuccess := false
	for _, collection := range w.Collections {
		it, err := client.UseDatabase(w.Database).Read(context.TODO(), collection, driver.Filter(`{}`), nil)
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
			//log.Debug().Msgf("read document '%s' '%s' '%v'", w.Database, collection, document)
			queueDoc.Add(document)
		}

		if it.Err() != nil {
			return false, it.Err()
		}

		existing := w.WorkloadData.Get(collection)
		if len(existing.Documents) != len(queueDoc.Documents) {
			log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks totalDocumentsInserted: %d, totalDocsRead: %d", w.Database, collection, len(existing.Documents), len(queueDoc.Documents))
		}

		isSuccess = reflect.DeepEqual(existing.Documents, queueDoc.Documents)
		if !isSuccess {
			for _, doc := range existing.Documents {
				log.Debug().Msgf("existing document %v", doc)
			}
			for _, doc := range queueDoc.Documents {
				log.Debug().Msgf("found document %v", doc)
			}

			log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks %v %v", w.Database, collection, existing.Documents, queueDoc.Documents)
			return false, nil
		}
	}

	return true, nil
}
