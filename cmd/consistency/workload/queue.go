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
