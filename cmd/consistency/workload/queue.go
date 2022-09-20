// Copyright 2022 Tigris Data, Inc.
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

import "sync"

type Queue struct {
	sync.Mutex

	collectionToDocument map[string]*QueueDocuments
}

func NewQueue(collections []string) *Queue {
	q := &Queue{
		collectionToDocument: make(map[string]*QueueDocuments),
	}
	for _, c := range collections {
		qd := NewQueueDocuments(c)
		q.collectionToDocument[c] = qd
	}

	return q
}

func (q *Queue) Get(collectionName string) *QueueDocuments {
	q.Lock()
	defer q.Unlock()
	return q.collectionToDocument[collectionName]
}

func (q *Queue) Add(collectionName string, doc *Document) {
	q.Lock()
	defer q.Unlock()

	qd := q.collectionToDocument[collectionName]
	qd.Add(doc)
}

type QueueDocuments struct {
	sync.Mutex

	Collection string
	Documents  map[int64]*Document
}

func NewQueueDocuments(collection string) *QueueDocuments {
	return &QueueDocuments{
		Collection: collection,
		Documents:  make(map[int64]*Document),
	}
}

func (q *QueueDocuments) Add(doc *Document) {
	q.Lock()
	defer q.Unlock()

	q.Documents[doc.F1] = doc
}
