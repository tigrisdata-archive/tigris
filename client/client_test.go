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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Doc struct {
	K1 string
	K2 int
	D1 string
}

func testClient(t *testing.T, c client) {
	t.Skip("not implemented")

	ctx := context.TODO()

	_ = c.Drop(ctx, "db1", "t1")

	err := c.Create(ctx, "db1", "t1", "K1,K2")
	require.NoError(t, err)

	t1 := c.Use("db1", "t1")

	doc1 := Doc{K1: "vK1", K2: 1, D1: "vD1"}

	err = t1.Insert(ctx, doc1)
	require.NoError(t, err)

	doc2, doc3 := Doc{K1: "vK1", K2: 2, D1: "vD2"}, Doc{K1: "vK2", K2: 1, D1: "vD3"}

	// multiple docs
	err = t1.Replace(ctx, doc1, doc2, doc3)
	require.NoError(t, err)

	// array of docs
	err = t1.Replace(ctx, []Doc{doc1, doc2, doc3})
	require.NoError(t, err)

	b, err := json.Marshal(doc1)
	require.NoError(t, err)

	// pre-marshaled data
	err = t1.Replace(ctx, b)
	require.NoError(t, err)

	// only key-part of docs may be filled for read and delete
	doc1.D1 = ""
	doc2.D1 = ""
	doc3.D1 = ""

	err = t1.Read(ctx, &doc1, &doc2, &doc3)
	require.NoError(t, err)

	assert.Equal(t, "vD1", doc1.D1)
	assert.Equal(t, "vD2", doc2.D1)
	assert.Equal(t, "vD3", doc3.D1)

	err = t1.Delete(ctx, []Doc{doc1, doc2, doc3})
	require.NoError(t, err)

	err = c.Drop(ctx, "db1", "t1")
	require.NoError(t, err)
}

func testTxClient(t *testing.T, c client) {
	t.Skip("transactions not implemented")

	ctx := context.TODO()

	err := c.Create(ctx, "db1", "t2", "K1,K2")
	require.NoError(t, err)

	tx, err := c.BeginTx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	t2 := tx.Use("db1", "t2")

	doc1 := Doc{K1: "vK1", K2: 1, D1: "vD1"}

	err = t2.Insert(ctx, doc1)
	require.NoError(t, err)

	doc2, doc3 := Doc{K1: "vK1", K2: 2, D1: "vD2"}, Doc{K1: "vK2", K2: 1, D1: "vD3"}

	// multiple docs
	err = t2.Replace(ctx, doc1, doc2, doc3)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	err = c.Drop(ctx, "db1", "t2")
	require.NoError(t, err)
}

func TestGRPCClient(t *testing.T) {
	h, p := getTestServerHostPort()
	c, err := newGRPCClient(context.TODO(), h, p)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testClient(t, c)
}

func TestHTTPClient(t *testing.T) {
	h, p := getTestServerHostPort()
	c, err := newHTTPClient(context.TODO(), h, p)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testClient(t, c)
}

func TestTxGRPCClient(t *testing.T) {
	h, p := getTestServerHostPort()
	c, err := newGRPCClient(context.TODO(), h, p)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testTxClient(t, c)
}

func TestTxHTTPClient(t *testing.T) {
	h, p := getTestServerHostPort()
	c, err := newHTTPClient(context.TODO(), h, p)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testClient(t, c)
	testTxClient(t, c)
}
