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

type SearchOnlyWorkload struct {
	Threads      int16
	Records      int64
	Project      string
	Index        string
	Schema       []byte
	WorkloadData *Queue
}

func (*SearchOnlyWorkload) Type() string {
	return "search_only_workload"
}

func (w *SearchOnlyWorkload) Setup(client driver.Driver) error {
	w.WorkloadData = NewQueue([]string{w.Index})

	// cleanup first
	_, err := client.DeleteProject(context.TODO(), w.Project)
	if err != nil {
		log.Err(err).Msgf("delete project failure for ['%s'] ignoring error", w.Project)
	}

	_, err = client.CreateProject(context.TODO(), w.Project)
	if err != nil {
		log.Err(err).Msgf("create project failure for ['%s'] ignoring error", w.Project)
	}

	if err = client.UseSearch(w.Project).CreateOrUpdateIndex(context.TODO(), w.Index, w.Schema); err != nil {
		return fmt.Errorf("%w CreateOrUpdateIndex failure for ['%s'.'%s']", err, w.Project, w.Index)
	}

	return nil
}

func (w *SearchOnlyWorkload) Start(client driver.Driver) (int64, error) {
	var insertErr error
	var wg sync.WaitGroup
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		uniqueIdentifier := w.Records + int64(i)*w.Records
		go func(id int64) {
			defer wg.Done()
			for j := int64(0); j < w.Records; j++ {
				doc := NewDocumentV1(id)

				serialized, err := SerializeDocV1(doc)
				if err != nil {
					insertErr = multierror.Append(insertErr, err)
					return
				}

				resp, err := client.UseSearch(w.Project).CreateOrReplace(context.TODO(), w.Index, []driver.Document{serialized})
				if err != nil {
					insertErr = multierror.Append(insertErr, fmt.Errorf("%w create document failure ['%s'.'%s']", err,
						w.Project, w.Index))
					return
				}
				for _, r := range resp {
					if r.Error != nil {
						insertErr = multierror.Append(insertErr, fmt.Errorf("create document failure "+
							"['%s'.'%s'] message: ['%s']", w.Project, w.Index, r.Error.Message))
						return
					}
				}

				w.WorkloadData.Add(w.Index, doc)
				id++
			}
		}(uniqueIdentifier)
	}
	wg.Wait()

	return w.Records * int64(w.Threads), insertErr
}

func (w *SearchOnlyWorkload) Check(client driver.Driver) (bool, error) {
	isSuccess := false
	it, err := client.UseSearch(w.Project).Search(context.TODO(), w.Index, &driver.SearchRequest{Q: "*"})
	if err != nil {
		return false, fmt.Errorf("%w reading from search index failure ['%s'.'%s']", err, w.Project, w.Index)
	}

	queueDoc := NewQueueDocuments(w.Index)
	var pageIt driver.SearchIndexResponse
	for it.Next(&pageIt) {
		for _, h := range pageIt.Hits {
			var document DocumentV1
			err := Deserialize(h.Data, &document)
			if err != nil {
				return false, fmt.Errorf("%w deserialzing document failed", err)
			}
			queueDoc.Add(&document)
		}
	}

	if it.Err() != nil {
		return false, it.Err()
	}

	existing := w.WorkloadData.Get(w.Index)
	if len(existing.Documents) != len(queueDoc.Documents) {
		log.Debug().Msgf("consistency issue for index ['%s'.'%s'], ignoring further checks "+
			"totalDocumentsInserted: %d, totalDocsRead: %d", w.Project, w.Index, len(existing.Documents), len(queueDoc.Documents))
	}

	isSuccess = reflect.DeepEqual(existing.Documents, queueDoc.Documents)
	if !isSuccess {
		log.Debug().Msgf("consistency issue for index ['%s'.'%s'], ignoring further checks", w.Project, w.Index)
		return false, nil
	}

	return true, nil
}
