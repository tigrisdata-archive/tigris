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
	"container/heap"

	"github.com/pkg/errors"
)

// ErrEmpty is returned for queues with no items
var ErrEmpty = errors.New("queue is empty")

type PriorityQueue[T any] struct {
	queue queue[T]
}

// NewPriorityQueue initializes internal data structure and returns new
// PriorityQueue accepting a comparator function
// comp function decides ordering in queue, Sort `this` before `that` if True.
func NewPriorityQueue[T any](comp func(this, that *T) bool) *PriorityQueue[T] {
	return &PriorityQueue[T]{queue: queue[T]{
		data:       make([]*T, 0),
		comparator: comp,
	}}
}

// Len returns items in queue
func (pq *PriorityQueue[T]) Len() int {
	return pq.queue.Len()
}

// Pop pops the highest priority item from queue
// The complexity is O(log n) where n = h.Len().
func (pq *PriorityQueue[T]) Pop() (*T, error) {
	if pq.Len() < 1 {
		return nil, ErrEmpty
	}
	item := heap.Pop(&pq.queue).(*T)
	return item, nil
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func (pq *PriorityQueue[T]) Push(x *T) {
	// Copy the item value(s) so that modifications to the source item does not
	// affect the item on the queue
	clone := *x

	heap.Push(&pq.queue, &clone)
}

// queue is the internal data structure used to satisfy heap.Interface and not
// supposed to be used directly. Use PriorityQueue instead.
type queue[T any] struct {
	data       []*T
	comparator func(this, that *T) bool
}

func (q queue[T]) Len() int {
	return len(q.data)
}

func (q queue[T]) Less(i, j int) bool {
	return q.comparator(q.data[i], q.data[j])
}

func (q queue[T]) Swap(i, j int) {
	q.data[i], q.data[j] = q.data[j], q.data[i]
}

func (q *queue[T]) Push(x any) {
	item := x.(*T)
	q.data = append(q.data, item)
}

func (q *queue[T]) Pop() any {
	old := q.data
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	q.data = old[0 : n-1]
	return item
}
