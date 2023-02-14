// Copyright 2022-2023 Tigris Data, Inc.
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

package metadata

import (
	"math/rand"
	"testing"
	"time"
)

// NameRegistry is used by tests to inject table names that can be used by tests.
// NameRegistry provides the names of the internal tables(subspaces) maintained by the metadata package. The interface
// helps in creating test tables for these structures.
type NameRegistry struct {
	// ReservedSubspaceName is the name of the table(subspace) where all the counters are stored.
	ReserveSB string
	// EncodingSubspaceName is the name of the table(subspace) which is used by the dictionary encoder to store all the
	// dictionary encoded values.
	EncodingSB string
	// SchemaSubspaceName (the schema subspace) will be storing the actual schema of the user for a collection. The schema subspace will
	// look like below
	//    ["schema", 0x01, x, 0x01, 0x03, "created", 0x01] => {"title": "t1", properties: {"a": int}, primary_key: ["a"]}
	//
	//  where,
	//    - schema is the keyword for this table.
	//    - 0x01 is the schema subspace version
	//    - x is the value assigned for the namespace
	//    - 0x01 is the value for the database.
	//    - 0x03 is the value for the collection.
	//    - "created" is keyword.
	//
	SchemaSB    string
	SearchSB    string
	UserSB      string
	NamespaceSB string
	ClusterSB   string
	VersionKey  string

	BaseCounterValue uint32
}

var DefaultNameRegistry = &NameRegistry{
	ReserveSB:   "reserved",
	EncodingSB:  "encoding",
	SchemaSB:    "schema",
	SearchSB:    "search_schema",
	UserSB:      "user",
	NamespaceSB: "namespace",
	ClusterSB:   "cluster",

	BaseCounterValue: reservedBaseValue,
}

func (d *NameRegistry) ReservedSubspaceName() []byte {
	return []byte(d.ReserveSB)
}

func (d *NameRegistry) EncodingSubspaceName() []byte {
	return []byte(d.EncodingSB)
}

func (d *NameRegistry) SchemaSubspaceName() []byte {
	return []byte(d.SchemaSB)
}

func (d *NameRegistry) SearchSchemaSubspaceName() []byte {
	return []byte(d.SearchSB)
}

func (d *NameRegistry) UserSubspaceName() []byte {
	return []byte(d.UserSB)
}

func (d *NameRegistry) NamespaceSubspaceName() []byte {
	return []byte(d.NamespaceSB)
}

func (d *NameRegistry) ClusterSubspaceName() []byte {
	return []byte(d.ClusterSB)
}

func (d *NameRegistry) GetVersionKey() []byte {
	return []byte(d.VersionKey)
}

func newTestNameRegistry(t *testing.T) *NameRegistry {
	r := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	s := t.Name()

	return &NameRegistry{
		ReserveSB:   "test_reserved_" + s,
		EncodingSB:  "test_encoding_" + s,
		SchemaSB:    "test_schema_" + s,
		SearchSB:    "test_search_schema_" + s,
		UserSB:      "test_user_" + s,
		NamespaceSB: "test_namespace_" + s,
		ClusterSB:   "test_cluster_" + s,
		VersionKey:  "test_version_key" + s,

		BaseCounterValue: r.Uint32(),
	}
}
