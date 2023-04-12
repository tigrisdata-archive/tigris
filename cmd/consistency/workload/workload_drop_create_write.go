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
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
)

type DropCreateWriteWorkload struct {
	Threads     int16
	Database    string
	Collections []string
	Schemas     [][]byte
	Records     int64
}

func (w *DropCreateWriteWorkload) Setup(client driver.Driver) error {
	_, err := client.DeleteProject(context.TODO(), w.Database)
	if err != nil {
		log.Err(err).Msgf("delete project failed, ignoring error '%s'", w.Database)
	}
	return nil
}

func (w *DropCreateWriteWorkload) Start(client driver.Driver) (int64, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 32; i++ {
			_, err := client.CreateProject(context.TODO(), w.Database)
			if err != nil {
				log.Err(err).Msgf("create project failed ignoring error '%s'", w.Database)
			}
			db := client.UseDatabase(w.Database)
			if err = db.CreateOrUpdateCollection(context.TODO(), w.Collections[0], w.Schemas[0]); err != nil {
				panic(err)
			}

			time.Sleep(1 * time.Second)

			if err = db.DropCollection(context.TODO(), w.Collections[0]); err != nil {
				panic(err)
			}
			time.Sleep(1 * time.Second)
		}
	}()
	wg.Wait()

	var workloadErr error
	db := client.UseDatabase(w.Database)
	for i := int16(0); i < w.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := int64(0); j < w.Records; j++ {
				doc := NewDocument(1)
				serialized, err := Serialize(doc)
				if err != nil {
					workloadErr = multierror.Append(workloadErr, err)
					return
				}
				if _, err = db.Replace(context.TODO(), w.Collections[0], []driver.Document{serialized}); err != nil {
					if e, ok := err.(*driver.Error); ok && (e.Code == api.Code_NOT_FOUND || e.Code == api.Code_DEADLINE_EXCEEDED) {
						continue
					}
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	return w.Records * int64(w.Threads), workloadErr
}

func (w *DropCreateWriteWorkload) Check(_ driver.Driver) (bool, error) {
	return true, nil
}

func (w *DropCreateWriteWorkload) Type() string {
	return "drop_create_write"
}
