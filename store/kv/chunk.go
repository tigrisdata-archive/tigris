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

package kv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/tigrisdata/tigris/internal"
)

const (
	KB   = 1000
	KB99 = 99 * KB

	chunkIdentifier = "_C_"
)

var chunkSize = KB99

type chunkCB func(chunkNo int32, chunkData []byte) error

// ChunkTxStore is used as a layer on top of KeyValueTxStore. The idea is that chunk store will automatically split the
// user payload if it is greater than 99KB. It adds some metadata in the zeroth chunk like total chunks so that it
// can easily merge the value again. The attributes of data passed by the caller is only needed in the first chunk, the
// remaining chunks only have body. The chunk number is appended at the end in the format "__<chunk number>". This number
// is used during merging from the key so there is no information apart from total chunk is persisted in the value.
type ChunkTxStore struct {
	TxStore

	enabled bool
}

func NewChunkStore(store TxStore, enabled bool) TxStore {
	return &ChunkTxStore{TxStore: store, enabled: enabled}
}

func (store *ChunkTxStore) BeginTx(ctx context.Context) (Tx, error) {
	btx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &ChunkTx{
		KeyValueTx: btx.(*KeyValueTx),
		enabled:    store.enabled,
	}, nil
}

func (tx *ChunkTx) executeChunks(data *internal.TableData, cb chunkCB) error {
	originalDoc := data.RawData
	chunk := int32(0)
	for start := 0; start < len(originalDoc); start += chunkSize {
		end := start + chunkSize
		if end > len(originalDoc) {
			end = len(originalDoc)
		}

		if err := cb(chunk, originalDoc[start:end]); err != nil {
			return err
		}
		chunk++
	}

	return nil
}

func (tx *ChunkTx) isChunkingNeeded(data *internal.TableData) bool {
	// if the payload size is greater than chunk size and chunking is enabled
	return data.ActualUserPayloadSize() > int32(chunkSize) && tx.enabled
}

type ChunkTx struct {
	*KeyValueTx
	enabled bool
}

func (tx *ChunkTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	if !tx.isChunkingNeeded(data) {
		return tx.KeyValueTx.Insert(ctx, table, key, data)
	}

	data.SetTotalChunks(int32(chunkSize))

	return tx.executeChunks(data, func(chunk int32, chunkData []byte) error {
		chunkKey := key
		if chunk == 0 {
			// first part
			return tx.KeyValueTx.Insert(ctx, table, chunkKey, data.CloneWithAttributesOnly(chunkData))
		}

		// we don't care about any other attributes for this case
		chunked := internal.NewTableData(chunkData)
		// int is encoded as int64 by FDB client so better use int64 to avoid any confusion.
		chunkKey = append(chunkKey, []KeyPart{chunkIdentifier, int64(chunk)}...)

		return tx.KeyValueTx.Replace(ctx, table, chunkKey, chunked, false)
	})
}

func (tx *ChunkTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	if !tx.isChunkingNeeded(data) {
		return tx.KeyValueTx.Replace(ctx, table, key, data, isUpdate)
	}

	data.SetTotalChunks(int32(chunkSize))

	// cleanup the existing range
	if err := tx.KeyValueTx.Delete(ctx, table, key); err != nil {
		return err
	}

	return tx.executeChunks(data, func(chunk int32, chunkData []byte) error {
		var chunked *internal.TableData
		chunkKey := key
		if chunk == 0 {
			// first part
			chunked = data.CloneWithAttributesOnly(chunkData)
		} else {
			chunked = internal.NewTableData(chunkData)
			// int is encoded as int64 by FDB client so better use int64 to avoid any confusion.
			chunkKey = append(chunkKey, []KeyPart{chunkIdentifier, int64(chunk)}...)
		}

		return tx.KeyValueTx.Replace(ctx, table, chunkKey, chunked, isUpdate)
	})
}

// Read needs to return chunk iterator so that it can merge and returned merged chunk to caller.
func (tx *ChunkTx) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iterator, err := tx.KeyValueTx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}

	return &ChunkIterator{
		Iterator: iterator,
	}, nil
}

func (tx *ChunkTx) ReadRange(ctx context.Context, table []byte, lKey Key, rKey Key, isSnapshot bool) (Iterator, error) {
	iterator, err := tx.KeyValueTx.ReadRange(ctx, table, lKey, rKey, isSnapshot)
	if err != nil {
		return nil, err
	}

	return &ChunkIterator{
		Iterator: iterator,
	}, nil
}

type ChunkIterator struct {
	Iterator

	err error
}

func (it *ChunkIterator) Next(value *KeyValue) bool {
	if !it.Iterator.Next(value) {
		return false
	}

	if !value.Data.IsChunkedData() {
		return true
	}

	var buf bytes.Buffer
	buf.Write(value.Data.RawData)
	hasNext := false
	chunk := int32(1)
	for ; chunk < *value.Data.TotalChunks; chunk++ {
		var chunked KeyValue
		if hasNext = it.Iterator.Next(&chunked); !hasNext {
			break
		}

		if it.validChunkKey(chunked.Key, chunk); it.Err() != nil {
			return false
		}

		buf.Write(chunked.Data.RawData)
	}
	if it.Iterator.Err() != nil {
		// there can be an error in between so we need to return that error
		it.err = it.Iterator.Err()
		return false
	}

	if chunk != *value.Data.TotalChunks {
		it.err = fmt.Errorf("mismatch in total chunk read '%d' versus total chunks expected '%d'",
			chunk, *value.Data.TotalChunks)
		return false
	}

	value.Data.RawData = buf.Bytes()
	return hasNext
}

func (it *ChunkIterator) validChunkKey(key Key, expChunk int32) {
	switch {
	case len(key) < 2:
		it.err = fmt.Errorf("key shorter than expected chunked key '%v'", key)
	case key[len(key)-2] != chunkIdentifier:
		it.err = fmt.Errorf("chunk identifier not found in the key '%v'", key)
	default:
		if chunkNo, ok := key[len(key)-1].(int64); !ok || expChunk != int32(chunkNo) {
			it.err = fmt.Errorf("chunk number mismatch found: '%v' exp: '%d'", key[len(key)-1], expChunk)
		}
	}
}

func (it *ChunkIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Err()
}
