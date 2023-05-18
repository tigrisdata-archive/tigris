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
	"context"

	"github.com/klauspost/compress/zstd"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

const (
	// To avoid impact on latencies for smaller payloads. This can be tuned.
	minCompressionThreshold = KB
)

const (
	zstdLevel2 = zstd.SpeedDefault
)

var (
	zStdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstdLevel2))
	zStdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

type CompressTxStore struct {
	TxStore

	enabled bool
}

func NewCompressionStore(store TxStore, enabled bool) TxStore {
	return &CompressTxStore{
		TxStore: store,
		enabled: enabled,
	}
}

func (store *CompressTxStore) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := store.TxStore.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &CompressTx{
		Tx:      tx,
		enabled: store.enabled,
	}, nil
}

type CompressTx struct {
	Tx

	enabled bool
}

func (tx *CompressTx) shouldCompress(data *internal.TableData) bool {
	minCompThreshold := config.DefaultConfig.KV.MinCompressThreshold
	if minCompThreshold <= 0 {
		minCompThreshold = minCompressionThreshold
	}

	return data.ActualUserPayloadSize() > minCompThreshold && tx.enabled
}

func (tx *CompressTx) compress(data *internal.TableData) *internal.TableData {
	if !tx.shouldCompress(data) {
		return data
	}

	var compressed []byte
	compressed = zStdEncoder.EncodeAll(data.RawData, compressed)
	compressedData := data.CloneWithAttributesOnly(compressed)
	compressionType := int32(zstdLevel2)
	compressedData.Compression = &compressionType

	return compressedData
}

func (tx *CompressTx) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	return tx.Tx.Insert(ctx, table, key, tx.compress(data))
}

func (tx *CompressTx) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData, isUpdate bool) error {
	return tx.Tx.Replace(ctx, table, key, tx.compress(data), isUpdate)
}

func (tx *CompressTx) Read(ctx context.Context, table []byte, key Key, reverse bool) (Iterator, error) {
	iterator, err := tx.Tx.Read(ctx, table, key, reverse)
	if err != nil {
		return nil, err
	}

	return &DecompressIterator{
		Iterator: iterator,
	}, nil
}

func (tx *CompressTx) ReadRange(ctx context.Context, table []byte, lKey Key, rKey Key, isSnapshot bool, reverse bool) (Iterator, error) {
	iterator, err := tx.Tx.ReadRange(ctx, table, lKey, rKey, isSnapshot, reverse)
	if err != nil {
		return nil, err
	}

	return &DecompressIterator{
		Iterator: iterator,
	}, nil
}

type DecompressIterator struct {
	Iterator

	err error
}

func (it *DecompressIterator) Next(value *KeyValue) bool {
	if !it.Iterator.Next(value) {
		return false
	}

	if value.Data.Compression == nil {
		return true
	}

	uncompressed, err := zStdDecoder.DecodeAll(value.Data.RawData, nil)
	if err != nil {
		it.err = err
		return false
	}
	value.Data.RawData = uncompressed
	return true
}

func (it *DecompressIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.Iterator.Err()
}
