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
	"reflect"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
)

type BigPayloadWorkload struct {
	Threads      int16
	Records      int64
	Database     string
	Collections  string
	Schemas      []byte
	WorkloadData *Queue
}

func (w *BigPayloadWorkload) Type() string {
	return "compression_workload"
}

func (w *BigPayloadWorkload) Setup(client driver.Driver) error {
	w.WorkloadData = NewQueue([]string{w.Collections})

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
		return errors.Wrapf(err, "begin tx failed for db '%s'", w.Database)
	}

	if err = tx.CreateOrUpdateCollection(ctx, w.Collections, w.Schemas); err != nil {
		return errors.Wrapf(err, "CreateOrUpdateCollection failed for db '%s' coll '%s'", w.Database, w.Collections)
	}

	return tx.Commit(ctx)
}

func (w *BigPayloadWorkload) Start(client driver.Driver) (int64, error) {
	var insertErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		uniqueIdentifier := w.Records + int64(i)*w.Records + 10000
		go func(id int64) {
			defer wg.Done()
			for j := int64(0); j < w.Records; j++ {
				var documents []driver.Document
				for k := 0; k < 10; k++ {
					// batch 10 documents and then write. Each document is around 550KB
					doc := NewDocumentV1(id)

					serialized, err := SerializeDocV1(doc)
					if err != nil {
						insertErr = multierror.Append(insertErr, err)
						return
					}

					documents = append(documents, serialized)
					w.WorkloadData.Add(w.Collections, doc)
				}

				if _, err := client.UseDatabase(w.Database).Replace(context.TODO(), w.Collections, documents); err != nil {
					insertErr = multierror.Append(insertErr, errors.Wrapf(err, "insert to collection failed '%s' '%s'", w.Database, w.Collections))
					return
				}

				id += 10
			}
		}(uniqueIdentifier)
	}
	wg.Wait()

	return w.Records * int64(w.Threads), insertErr
}

func (w *BigPayloadWorkload) Check(client driver.Driver) (bool, error) {
	isSuccess := false
	it, err := client.UseDatabase(w.Database).Read(context.TODO(), w.Collections, driver.Filter(`{}`), nil)
	if err != nil {
		return false, errors.Wrapf(err, "read to collection failed '%s' '%s'", w.Database, w.Collections)
	}

	queueDoc := NewQueueDocuments(w.Collections)
	var doc driver.Document
	for it.Next(&doc) {
		var document DocumentV1
		err := Deserialize(doc, &document)
		if err != nil {
			return false, errors.Wrapf(err, "deserialzing document failed")
		}
		// log.Debug().Msgf("read document '%s' '%s' '%v'", w.Database, collection, document)
		queueDoc.Add(&document)
	}

	if it.Err() != nil {
		return false, it.Err()
	}

	existing := w.WorkloadData.Get(w.Collections)
	if len(existing.Documents) != len(queueDoc.Documents) {
		log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks totalDocumentsInserted: %d, totalDocsRead: %d", w.Database, w.Collections, len(existing.Documents), len(queueDoc.Documents))
	}

	isSuccess = reflect.DeepEqual(existing.Documents, queueDoc.Documents)
	if !isSuccess {
		log.Debug().Msgf("consistency issue for collection '%s' '%s', ignoring further checks", w.Database, w.Collections)
		return false, nil
	}

	return true, nil
}
