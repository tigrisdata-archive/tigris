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

package main

import (
	"fmt"
	"strings"
)

const (
	IndexNamespace      = "tigrisdb"
	ClusteringIndexName = "clustering"
)

type Table struct {
	Name string
	Key  []string
}

var tables = map[string]Table{}

func GetTableName(db string, table string) string {
	return fmt.Sprintf(IndexNamespace+".%s.%s", db, table)
}

func GetIndexName(db string, table string, index string) string {
	return fmt.Sprintf(IndexNamespace+".%s.%s.index.%s", db, table, index)
}

func AddTable(name string, key string) {
	keys := strings.Split(key, ",")

	tables[name] = Table{Name: name, Key: keys}
}

func GetTableKey(name string) []string {
	if _, ok := tables[name]; !ok {
		return nil
	}
	return tables[name].Key
}
