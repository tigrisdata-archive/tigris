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
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
)

type DDLWorkload struct {
	Threads     int16
	Database    string
	Collections []string
	Schemas     [][]byte
}

func (*DDLWorkload) Type() string {
	return "ddl_workload"
}

func (w *DDLWorkload) Setup(client driver.Driver) error {
	// cleanup first
	_, err := client.DeleteProject(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("delete project failed, ignoring error '%s'", w.Database)
	}

	_, err = client.CreateProject(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("creation of project failed ignoring error '%s'", w.Database)
	}

	return nil
}

func (*DDLWorkload) Start(_ driver.Driver) (int64, error) { return 0, nil }

func (w *DDLWorkload) Check(client driver.Driver) (bool, error) {
	var wg sync.WaitGroup
	var ddlErr error

	isConsistent := true
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		go func(iteration int16) {
			defer wg.Done()
			consistent, err := w.validate(iteration, client, true)
			if err != nil {
				ddlErr = multierror.Append(ddlErr, err)
			}
			if !consistent {
				isConsistent = consistent
			}
		}(i)
	}
	wg.Wait()
	if ddlErr != nil {
		return false, ddlErr
	}
	if !isConsistent {
		return isConsistent, nil
	}

	// Cleanup the collection and try now rollback scenario
	_ = client.UseDatabase(w.Database).DropCollection(context.TODO(), w.Collections[0])

	wg = sync.WaitGroup{}
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		go func(iteration int16) {
			defer wg.Done()
			consistent, err := w.validate(iteration, client, false)
			if err != nil {
				ddlErr = multierror.Append(ddlErr, err)
			}
			if !consistent {
				isConsistent = consistent
			}
		}(i)
	}
	wg.Wait()

	return isConsistent, ddlErr
}

func (w *DDLWorkload) validate(iteration int16, client driver.Driver, isCommit bool) (bool, error) {
	var err error
	tx, err := client.UseDatabase(w.Database).BeginTx(context.TODO())
	if err != nil {
		return false, fmt.Errorf("%w begin tx failed for db '%s'", err, w.Database)
	}

	_ = tx.DropCollection(context.TODO(), w.Collections[0])

	collections, err := tx.ListCollections(context.TODO())
	if err != nil {
		return false, fmt.Errorf("%w list collection failed for db '%s'", err, w.Database)
	}

	isExists := w.isCollectionExists(collections, w.Collections[0])
	if isExists {
		log.Debug().Msgf("expected list collection to not contain the collection %s", w.Collections[0])
		return false, nil
	}

	for i := 0; i < 10; i++ {
		err = tx.CreateOrUpdateCollection(context.TODO(), w.Collections[0], w.Schemas[0])
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "concurrent create collection request, aborting") {
			time.Sleep(time.Duration(i*100+rand.Intn(100)) * time.Millisecond) //nolint:gosec
			continue
		}

		return false, fmt.Errorf("%w create collection failed '%s'", err, w.Collections[0])
	}
	if err != nil {
		// ignore this thread check, concurrency is causing conflicts
		//nolint:nilerr
		return true, nil
	}

	collections, err = tx.ListCollections(context.TODO())
	if err != nil {
		return false, fmt.Errorf("%w list collection failed '%s'", err, w.Database)
	}

	log.Debug().Msgf("iteration %d collections %v", iteration, collections)
	isExists = w.isCollectionExists(collections, w.Collections[0])
	if !isExists {
		log.Debug().Msgf("expected list collection to contain the collection %d %s %v", iteration, w.Collections[0], collections)
		return false, nil
	}

	if isCommit {
		if err = tx.Commit(context.TODO()); err != nil {
			log.Debug().Err(err).Msgf("commit error for collection %s", w.Collections[0])
		}
	} else {
		if err = tx.Rollback(context.TODO()); err != nil {
			log.Debug().Err(err).Msgf("rollback error for collection %s", w.Collections[0])
		}
	}

	collections, err = client.UseDatabase(w.Database).ListCollections(context.TODO())
	if err != nil {
		log.Debug().Err(err).Msgf("list collections error for collection %s", w.Collections[0])
	}

	isExists = w.isCollectionExists(collections, w.Collections[0])
	if isCommit {
		// should be visible outside the transaction as well.
		if !isExists {
			log.Debug().Msgf("expected collection to be visible outside the transaction %s %v", w.Collections[0], collections)
			return false, nil
		}
	} else {
		if isExists {
			log.Debug().Msgf("expected collection to be not visible outside the transaction %s %v", w.Collections[0], collections)
			return false, nil
		}
	}

	return true, nil
}

func (*DDLWorkload) isCollectionExists(collections []string, queryingAbout string) bool {
	for _, c := range collections {
		if c == queryingAbout {
			return true
		}
	}

	return false
}
