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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/transaction"
)

func TestNamespacesSubspace(t *testing.T) {
	t.Run("put_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		namespacePayload := []byte(`{
								"tier": "1",
								"contact": "abc@abc.com"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.Equal(t, errors.InvalidArgument("invalid empty metadataKey"), n.InsertNamespaceMetadata(ctx, tx, 1, "", namespacePayload))
		require.Equal(t, errors.InvalidArgument("invalid nil payload"), n.InsertNamespaceMetadata(ctx, tx, 1, "some-valid-metadata-id", nil))
		require.Equal(t, errors.InvalidArgument("invalid namespace, id must be greater than 0"), n.InsertNamespaceMetadata(ctx, tx, 0, "meta-key-1", namespacePayload))
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})

	t.Run("put_get_1", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		namespacePayload := []byte(`{
								"tier": "1",
								"contact": "abc@abc.com"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-1", namespacePayload))
		value, err := n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, namespacePayload, value)

		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})

	t.Run("put_get_2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		namespacePayload := []byte(`{
								"tier": "1",
								"contact": "abc@abc.com"
								}`)
		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-1", namespacePayload))
		value, err := n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, namespacePayload, value)

		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})

	t.Run("put_get_update_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		namespacePayload := []byte(`{
								"tier": "1",
								"contact": "abc@abc.com"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-1", namespacePayload))
		value, err := n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, namespacePayload, value)

		updatedNamespacePayload := []byte(`{
								"tier": "2",
								"contact": "xyz@abc.com"
								}`)
		require.NoError(t, n.UpdateNamespaceMetadata(ctx, tx, 1, "meta-key-1", updatedNamespacePayload))
		value, err = n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, updatedNamespacePayload, value)

		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})

	t.Run("put_get_deletenamespace_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		metaVal1 := []byte(`{
								"meta-key-1": "val1",
								"meta-key-2": "val2"
								}`)

		metaVal2 := []byte(`{
								"meta-key-1": "val1",
								"meta-key-2": "val2"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-1", metaVal1))
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-2", metaVal2))

		metaValRetrieved, err := n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, metaVal1, metaValRetrieved)

		metaValRetrieved, err = n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-2")
		require.NoError(t, err)
		require.Equal(t, metaVal2, metaValRetrieved)

		require.NoError(t, n.DeleteNamespace(ctx, tx, 1))

		metaValRetrieved, err = n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Nil(t, metaValRetrieved)

		metaValRetrieved, err = n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-2")
		require.NoError(t, err)
		require.Nil(t, metaValRetrieved)

		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})

	t.Run("put_get_delete_get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		n := NewNamespaceStore(&TestMDNameRegistry{
			NamespaceSB: "test_namespace",
		})
		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())

		namespacePayload := []byte(`{
								"tier": "1",
								"contact": "abc@abc.com"
								}`)

		tm := transaction.NewManager(kvStore)
		tx, err := tm.StartTx(ctx)
		require.NoError(t, err)
		require.NoError(t, n.InsertNamespaceMetadata(ctx, tx, 1, "meta-key-1", namespacePayload))
		value, err := n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Equal(t, namespacePayload, value)

		require.NoError(t, n.DeleteNamespaceMetadata(ctx, tx, 1, "meta-key-1"))
		value, err = n.GetNamespaceMetadata(ctx, tx, 1, "meta-key-1")
		require.NoError(t, err)
		require.Nil(t, value)

		_ = kvStore.DropTable(ctx, n.NamespaceSubspaceName())
	})
}
