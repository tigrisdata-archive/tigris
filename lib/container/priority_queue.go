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

package container

import (
	"github.com/pkg/errors"
	"container/heap"
)

// ErrEmpty is returned for queues with no items
var ErrEmpty = errors.New("queue is empty")

type PriorityQueue[T any] struct {
	queue queue[T]
}

func NewPriorityQueue[T any](comp func(this, that *T) bool) *PriorityQueue[T] {
	return &PriorityQueue[T]{queue: queue[T]{
		data:       make([]*T, 0),
		comparator: comp,
	}}
}

func (pq *PriorityQueue[T]) Len() int {
	return pq.queue.Len()
}

func (pq *PriorityQueue[T]) Pop() (*T, error) {
	if pq.Len() < 1 {
		return nil, ErrEmpty
	}
	item := heap.Pop(&pq.queue).(*T)
	return item, nil
}

func (pq *PriorityQueue[T]) Push(i *T) error {
	// Copy the item value(s) so that modifications to the source item does not
	// affect the item on the queue
	clone := *i

	heap.Push(&pq.queue, &clone)
	return nil
}

type queue[T any] struct {
	data       []*T
	comparator func(this, that *T) bool
}

func (pq queue[T]) Len() int {
	return len(pq.data)
}

func (pq queue[T]) Less(i, j int) bool {
	return pq.comparator(pq.data[i], pq.data[j])
}

func (pq queue[T]) Swap(i, j int) {
	pq.data[i], pq.data[j] = pq.data[j], pq.data[i]
}

func (pq *queue[T]) Push(x any) {
	item := x.(*T)
	(*pq).data = append((*pq).data, item)
}

func (pq *queue[T]) Pop() any {
	old := (*pq).data
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	(*pq).data = old[0 : n-1]
	return item
}
