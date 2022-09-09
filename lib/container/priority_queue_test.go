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

		for _, doc := range documents {
			err := pq.Push(&doc)
			assert.NoError(t, err)
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

		for _, doc := range documents {
			err := pq.Push(&doc)
			assert.NoError(t, err)
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
		_ = pq.Push(&documents[0])
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
