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
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/projection"
	"github.com/tigrisdata/tigris-client-go/tigris"
	"github.com/tigrisdata/tigris-client-go/update"
)

func TestClientCollectionBasic(t *testing.T) {
	ctx := context.TODO()

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	type Coll2 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	h, p := getTestServerHostPort()
	db, err := tigris.OpenDatabase(ctx, &config.Database{Driver: config.Driver{URL: fmt.Sprintf("%v:%d", h, p)}}, "db111222", &Coll1{}, &Coll2{})
	require.NoError(t, err)

	c := tigris.GetCollection[Coll1](db)

	d1 := &Coll1{Key1: "aaa", Field1: 123}
	d2 := &Coll1{Key1: "bbb", Field1: 123}

	_, err = c.Insert(ctx, d1, d2)
	require.NoError(t, err)

	_, err = c.InsertOrReplace(ctx, d2)
	require.NoError(t, err)

	_, err = c.Update(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "bbb")),
		update.Set("Field1", 345),
	)
	require.NoError(t, err)

	it, err := c.Read(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
		projection.Exclude("Key1").
			Include("Field1"),
	)
	require.NoError(t, err)

	d1.Field1 = 345
	var d Coll1
	for it.Next(&d) {
		require.Equal(t, d1.Field1, d.Field1)
		require.Equal(t, "", d.Key1)
	}
	require.NoError(t, it.Err())

	it.Close()

	it, err = c.ReadAll(ctx, projection.All)
	require.NoError(t, err)

	pd, err := c.ReadOne(ctx, filter.Eq("Key1", "aaa"))
	require.NoError(t, err)
	require.Equal(t, d1, pd)

	_, err = c.Delete(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")))
	require.NoError(t, err)

	_, err = c.DeleteAll(ctx)
	require.NoError(t, err)

	err = c.Drop(ctx)
	require.NoError(t, err)
}

func TestClientCollectionTx(t *testing.T) {
	ctx := context.TODO()

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	h, p := getTestServerHostPort()
	db, err := tigris.OpenDatabase(ctx, &config.Database{Driver: config.Driver{URL: fmt.Sprintf("%v:%d", h, p)}}, "db111333", &Coll1{})
	require.NoError(t, err)

	err = db.Tx(ctx, func(ctx context.Context, tx *tigris.Tx) error {
		c := tigris.GetTxCollection[Coll1](tx)

		d1 := &Coll1{Key1: "aaa", Field1: 123}
		d2 := &Coll1{Key1: "bbb", Field1: 123}

		_, err := c.Insert(ctx, d1, d2)
		require.NoError(t, err)

		_, err = c.InsertOrReplace(ctx, d2)
		require.NoError(t, err)

		_, err = c.Update(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "bbb")),
			update.Set("Field1", 345),
		)
		require.NoError(t, err)

		it, err := c.Read(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")),
			projection.Exclude("Key1").
				Include("Field1"),
		)
		require.NoError(t, err)

		d1.Field1 = 345
		var d Coll1
		for it.Next(&d) {
			require.Equal(t, d1.Field1, d.Field1)
			require.Equal(t, "", d.Key1)
		}
		require.NoError(t, it.Err())

		it.Close()

		it, err = c.ReadAll(ctx, projection.All)
		require.NoError(t, err)

		_, err = c.DeleteAll(ctx)
		require.NoError(t, err)

		pd, err := c.ReadOne(ctx, filter.Eq("Key1", "aaa"))
		require.NoError(t, err)
		require.Equal(t, d1, pd)

		_, err = c.Delete(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")))
		require.NoError(t, err)

		_, err = c.Insert(ctx, &Coll1{Key1: "aaa", Field1: 567})
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	c := tigris.GetCollection[Coll1](db)
	it, err := c.ReadAll(ctx, projection.All)
	require.NoError(t, err)

	var d Coll1
	require.True(t, it.Next(&d))
	assert.Equal(t, Coll1{Key1: "aaa", Field1: 567}, d)
	require.True(t, it.Next(&d))
	assert.Equal(t, Coll1{Key1: "bbb", Field1: 345}, d)

	require.NoError(t, it.Err())

	err = c.Drop(ctx)
	require.NoError(t, err)
}
