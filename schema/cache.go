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

package schema

import (
	"sync"
)

type Cache struct {
	sync.RWMutex

	schemas map[string]*database
}

type database struct {
	collections map[string]Collection
}

func (d *database) put(c Collection) {
	d.collections[c.Name()] = c
}

func (d *database) get(collectionName string) Collection {
	return d.collections[collectionName]
}

func (d *database) remove(collectionName string) {
	delete(d.collections, collectionName)
}

func NewCache() *Cache {
	return &Cache{
		schemas: make(map[string]*database),
	}
}

func (c *Cache) Put(collection Collection) {
	c.Lock()
	defer c.Unlock()

	if db := c.schemas[collection.Database()]; db == nil {
		db = &database{
			collections: make(map[string]Collection),
		}
		db.put(collection)
		c.schemas[collection.Database()] = db
	}

	c.schemas[collection.Database()].put(collection)
}

func (c *Cache) Remove(databaseName, collectionName string) {
	c.Lock()
	defer c.Unlock()

	if db := c.schemas[databaseName]; db == nil {
		return
	}

	c.schemas[databaseName].remove(collectionName)
}

func (c *Cache) Get(databaseName, collectionName string) Collection {
	c.RLock()
	defer c.RUnlock()

	if db := c.schemas[databaseName]; db == nil {
		return nil
	}

	return c.schemas[databaseName].get(collectionName)
}
