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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	t.Run("smallest priority first", func(t *testing.T) {
		pq := NewPriorityQueue[document](func(this, that *document) bool {
			return this.Priority < that.Priority
		})

		for i := range documents {
			pq.Push(&documents[i])
		}

		expectedOrder := []int{1, 2, 3, 4, 5}
		for _, p := range expectedOrder {
			val, err := pq.Pop()
			assert.NoError(t, err)
			assert.Equal(t, p, val.Priority)
		}
	})

	t.Run("highest priority first", func(t *testing.T) {
		pq := NewPriorityQueue[document](func(this, that *document) bool {
			return this.Priority > that.Priority
		})

		for i := range documents {
			pq.Push(&documents[i])
		}

		expectedOrder := []int{5, 4, 3, 2, 1}
		for _, p := range expectedOrder {
			val, err := pq.Pop()
			assert.NoError(t, err)
			assert.Equal(t, p, val.Priority)
		}
	})

	t.Run("pop from empty queue", func(t *testing.T) {
		pq := NewPriorityQueue[document](func(this, that *document) bool {
			return this.Priority > that.Priority
		})
		val, err := pq.Pop()
		assert.Equal(t, ErrEmpty, err)
		assert.Nil(t, val)
	})

	t.Run("validate length works", func(t *testing.T) {
		pq := NewPriorityQueue[document](func(this, that *document) bool {
			return this.Priority > that.Priority
		})
		assert.Equal(t, pq.Len(), 0)
		pq.Push(&documents[0])
		assert.Equal(t, pq.Len(), 1)
		_, _ = pq.Pop()
		assert.Equal(t, pq.Len(), 0)
	})
}

type document struct {
	Value    string
	Priority int
}

var documents = []document{
	{"third", 3},
	{"fifth", 5},
	{"first", 1},
	{"fourth", 4},
	{"second", 2},
}
