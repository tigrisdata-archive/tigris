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

package metadata

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/schema"
	"github.com/tigrisdata/tigrisdb/server/metadata/encoding"
)

func TestEncodeKey(t *testing.T) {
	ns := NewTenantNamespace("hello", 1)
	db := &Database{id: 3}
	coll := &schema.DefaultCollection{Id: 5}
	idx := &schema.Index{Id: 10}

	k := NewEncoder()
	tbl := k.EncodeTableName(ns, db, coll, idx)
	fmt.Println(tbl)
	require.Equal(t, uint32(1), encoding.ByteToUInt32(tbl[0:4]))
	require.Equal(t, uint32(3), encoding.ByteToUInt32(tbl[4:8]))
	require.Equal(t, uint32(5), encoding.ByteToUInt32(tbl[8:12]))
	require.Equal(t, uint32(10), encoding.ByteToUInt32(tbl[12:16]))
}
