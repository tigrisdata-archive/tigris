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

package workload

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/hashicorp/go-multierror"
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

func (*ReplaceOnlyWorkload) Type() string {
	return "replace_only_workload"
}

func (w *ReplaceOnlyWorkload) Setup(client driver.Driver) error {
	w.WorkloadData = NewQueue(w.Collections)

	ctx := context.TODO()

	// cleanup first
	_, err := client.DeleteProject(ctx, w.Database)
	if err != nil {
		log.Err(err).Msgf("delete project failed, ignoring error '%s'", w.Database)
	}

	_, err = client.CreateProject(ctx, w.Database)
	if err != nil {
		log.Err(err).Msgf("create project failed ignoring error '%s'", w.Database)
	}

	tx, err := client.UseDatabase(w.Database).BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("%w begin tx failed for db '%s'", err, w.Database)
	}

	for i := 0; i < len(w.Schemas); i++ {
		if err = tx.CreateOrUpdateCollection(ctx, w.Collections[i], w.Schemas[i]); err != nil {
			return fmt.Errorf("%w CreateOrUpdateCollection failed for db '%s' coll '%s'", err, w.Database, w.Collections[i])
		}
	}

	return tx.Commit(ctx)
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
						replaceErr = multierror.Append(replaceErr, fmt.Errorf(" %w replace to collection failed '%s' '%s'", err, w.Database, w.Collections[k]))
						return
					}

					w.WorkloadData.Add(w.Collections[k], doc)
					// log.Debug().Msgf("replaced document '%s' '%s' '%v'\n", w.Database, w.Collections[k], doc)
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
			return false, fmt.Errorf("%w read to collection failed '%s' '%s'", err, w.Database, collection)
		}

		queueDoc := NewQueueDocuments(collection)
		var doc driver.Document
		for it.Next(&doc) {
			var document Document
			err := Deserialize(doc, &document)
			if err != nil {
				return false, fmt.Errorf("%w deserialzing document failed", err)
			}
			// log.Debug().Msgf("read document '%s' '%s' '%v'", w.Database, collection, document)
			queueDoc.Add(&document)
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
