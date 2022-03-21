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

//go:build integration

package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb-client-go/driver"
	"github.com/tigrisdata/tigrisdb/server/config"
)

func getTestServerHostPort() (string, int16) {
	config.LoadEnvironment() // Move this to test.Main

	if config.GetEnvironment() == config.EnvTest {
		return "tigris_server", 8081
	}
	return "localhost", 8081
}

func testRead(t *testing.T, c driver.Driver, filter driver.Filter, expected []driver.Document) {
	ctx := context.Background()

	it, err := c.Read(ctx, "db1", "c1", filter)
	require.NoError(t, err)

	var doc driver.Document
	var i int
	for it.Next(&doc) {
		assert.JSONEq(t, string(expected[i]), string(doc))
		i++
	}

	assert.NoError(t, it.Err())
}

func testClient(t *testing.T, c driver.Driver) {
	t.Skip("not implemented")

	ctx := context.TODO()

	_ = c.DropCollection(ctx, "db1", "c1")

	schema := `{ "schema": {
		"K1": "string",
		"K2": "int",
		"D1": "string",
		"primary_key":  [ "K1", "K2" ]
	}`

	err := c.CreateCollection(ctx, "db1", "c1", driver.Schema(schema))
	require.NoError(t, err)

	doc1 := driver.Document(`{"K1"": "vK1", "K2"": 1, "D1"": "vD1"}`)

	_, err = c.Insert(ctx, "db1", "c1", []driver.Document{doc1})
	require.NoError(t, err)

	doc2, doc3 := driver.Document(`{"K1"": "vK1", "K2"": 2, "D1"": "vD2"}`), driver.Document(`{"K1"": "vK1", "K2"": 3, "D1"": "vD3"}`)

	// multiple docs
	_, err = c.Insert(ctx, "db1", "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	// array of docs
	_, err = c.Insert(ctx, "db1", "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{ "$or" : [ {"K1" : "vK1", "K2" : 1}, {"K1" : "vk1", "K2" : 3}]}`),
		[]driver.Document{doc1, doc3})

	_, err = c.Delete(ctx, "db1", "c1", driver.Filter(`{ "$or" : [ {"K1" : "vK1", "K2" : 1}, {"K1" : "vk1", "K2" : 3}]}`))
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{}`), []driver.Document{doc2})

	err = c.DropCollection(ctx, "db1", "c1")
	require.NoError(t, err)
}

func testTxClient(t *testing.T, c driver.Driver) {
	t.Skip("not implemented")

	ctx := context.TODO()

	_ = c.DropCollection(ctx, "db1", "c1")

	schema := `{ "schema": {
		"K1": "string",
		"K2": "int",
		"D1": "string",
		"primary_key":  [ "K1", "K2" ]
	}`

	err := c.CreateCollection(ctx, "db1", "c1", driver.Schema(schema))
	require.NoError(t, err)

	tx, err := c.BeginTx(ctx, "db1")
	defer func() { _ = tx.Rollback(ctx) }()

	doc1 := driver.Document(`{"K1"": "vK1", "K2"": 1, "D1"": "vD1"}`)

	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1})
	require.NoError(t, err)

	doc2, doc3 := driver.Document(`{"K1"": "vK1", "K2"": 2, "D1"": "vD2"}`), driver.Document(`{"K1"": "vK1", "K2"": 3, "D1"": "vD3"}`)

	// multiple docs
	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	// array of docs
	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{ "$or" : [ {"K1" : "vK1", "K2" : 1}, {"K1" : "vk1", "K2" : 3}]}`),
		[]driver.Document{doc1, doc3})

	_, err = c.Delete(ctx, "db1", "c1", driver.Filter(`{ "$or" : [ {"K1" : "vK1", "K2" : 1}, {"K1" : "vk1", "K2" : 3}]}`))
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{}`), []driver.Document{doc2})

	_, err = c.Delete(ctx, "db1", "c1", driver.Filter(`{"K1" : "vK1", "K2" : 2}`))
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{}`), nil)

	tx, err = c.BeginTx(ctx, "db1")

	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1})
	require.NoError(t, err)

	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1, doc2, doc3})
	require.NoError(t, err)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	testRead(t, c, driver.Filter(`{}`), nil)

	err = c.DropCollection(ctx, "db1", "c1")
	require.NoError(t, err)
}

func TestGRPCClient(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.GRPC
	c, err := driver.NewDriver(context.Background(), fmt.Sprintf("%s:%d", h, p), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testClient(t, c)
}

func TestHTTPClient(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.HTTP
	c, err := driver.NewDriver(context.Background(), fmt.Sprintf("%s:%d", h, p), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testClient(t, c)
}

func TestTxGRPCClient(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.GRPC
	c, err := driver.NewDriver(context.Background(), fmt.Sprintf("%s:%d", h, p), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testTxClient(t, c)
}

func TestTxHTTPClient(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.HTTP
	c, err := driver.NewDriver(context.Background(), fmt.Sprintf("%s:%d", h, p), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testTxClient(t, c)
}
