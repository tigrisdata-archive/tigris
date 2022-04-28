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

func (q *Queue) Insert(collectionName string, doc *Document) {
	q.Lock()
	defer q.Unlock()

	qd := q.collectionToDocument[collectionName]
	qd.Insert(doc)
}

type QueueDocuments struct {
	sync.Mutex

	Collection string
	Documents  []*Document
}

func NewQueueDocuments(collection string) *QueueDocuments {
	return &QueueDocuments{
		Collection: collection,
	}
}

func (q *QueueDocuments) Insert(doc *Document) {
	q.Lock()
	defer q.Unlock()
	q.Documents = append(q.Documents, doc)
}
