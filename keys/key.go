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

package keys

// Key is an interface that provides an encoded key which will be used for storing Key, Value in FDB. The Key has two
// elements, the first set of bytes is the encoded table name and the remaining is the actual index values.
type Key interface {
	// Table is logical representation of namespace, database, collection and type of index.
	Table() []byte
	// IndexKeys has the value(s) of a single or composite index. The index keys are appended
	// to the table name to form the Key.
	IndexKeys() []interface{}
}

type tableEncodedKey struct {
	table     []byte
	indexKeys []interface{}
}

// NewKey returns the Key.
func NewKey(table []byte, indexKeys ...interface{}) Key {
	return &tableEncodedKey{
		table:     table,
		indexKeys: indexKeys,
	}
}

func (p *tableEncodedKey) Table() []byte {
	return p.table
}

func (p *tableEncodedKey) IndexKeys() []interface{} {
	return p.indexKeys
}
