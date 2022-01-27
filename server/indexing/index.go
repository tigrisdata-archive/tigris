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

package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/store/kv"
	"github.com/tigrisdata/tigrisdb/types"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
)

const (
	maxBatchSize  = 25
	partitionBits = 16
)

type Index struct {
	kv kv.KV
}

// NewIndexStore initializes micro-shard indexing store
func NewIndexStore(kv kv.KV) (*Index, error) {
	return &Index{kv: kv}, nil
}

func insertShardKey(ctx context.Context, table string, batch kv.Tx, mshard *api.MicroShardKey, key []byte, ts int64, fileID string, replace bool) error {
	dk := NewIndexKey(key, ts, fileID, mshard.GetOffset())

	var err error
	if replace {
		mshard.FileId = fileID
		mshard.Timestamp = ts
		data, err := json.Marshal(mshard)
		if ulog.E(err) {
			return err
		}
		err = batch.Replace(ctx, table, dk, data)
	} else {
		err = batch.Delete(ctx, table, dk)
	}

	return err
}

func (i *Index) processFileShards(ctx context.Context, table string, ts int64, fileID string, shards []*api.MicroShardKey, replace bool) error {
	batch := i.kv.Batch()
	nops := 0

	for _, v := range shards {
		if err := insertShardKey(ctx, table, batch, v, v.GetMinKey(), ts, fileID, replace); err != nil {
			return err
		}
		nops++

		log.Debug().Str("min_key", string(v.GetMinKey())).Str("max_key", string(v.GetMaxKey())).Msg("processFileShard")
		if bytes.Compare(v.GetMinKey(), v.GetMaxKey()) != 0 {
			if err := insertShardKey(ctx, table, batch, v, v.GetMaxKey(), ts, fileID, replace); err != nil {
				return err
			}
			nops++
		}

		if nops >= maxBatchSize-1 {
			if err := batch.Commit(ctx); err != nil {
				return err
			}
			batch = i.kv.Batch()
		}
	}

	if nops != 0 {
		if err := batch.Commit(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (i *Index) ReplaceMicroShardFile(ctx context.Context, table string, del *api.ShardFile, add []*api.ShardFile) error {
	// Insert new shards
	for _, f := range add {
		if err := i.processFileShards(ctx, table, f.GetTimestamp(), f.GetFileId(), f.GetShards(), true); ulog.E(err) {
			return err
		}
	}

	// Now delete old shards
	if err := i.processFileShards(ctx, table, del.GetTimestamp(), del.GetFileId(), del.GetShards(), false); ulog.E(err) {
		return err
	}

	return nil
}

func (i *Index) readOne(ctx context.Context, table string, key types.Key) ([]kv.Doc, error) {
	docs, err := i.kv.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}

	if len(docs) != 0 {
		return docs, nil
	}

	docs, err = i.kv.ReadRange(ctx, table, key.Partition(), key, nil, 1)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (i *Index) readRange(ctx context.Context, table string, lk types.Key, rk types.Key) ([]kv.Doc, error) {
	log.Debug().Str("lkPartition", string(lk.Partition())).Str("rkPartition", string(rk.Partition())).Msg("readRange")
	log.Debug().Str("lKey", string(lk.Primary())).Str("rKey", string(rk.Primary())).Msg("readRange")

	// both bound belong to the same partition
	if bytes.Compare(lk.Partition(), rk.Partition()) == 0 {
		docs, err := i.kv.ReadRange(ctx, table, lk.Partition(), lk, rk, 0)
		if err != nil {
			return nil, err
		}

		if len(docs) != 0 {
			return docs, nil
		}

		docs, err = i.kv.ReadRange(ctx, table, lk.Partition(), lk, nil, 1)
		if err != nil {
			return nil, err
		}

		return docs, nil
	}

	//read first partial partition
	docs, err := i.kv.ReadRange(ctx, table, lk.Partition(), lk, nil, 0)
	if err != nil {
		return nil, err
	}

	//read all the partitions in between
	it, err := types.NewPrefixPartitionKeyIterator(lk.Partition(), partitionBits)
	if err != nil {
		return nil, err
	}

	for p := it.Next(); bytes.Compare(p, rk.Partition()) != 0; p = it.Next() {
		pdocs, err := i.kv.ReadRange(ctx, table, p, nil, nil, 0)
		if err != nil {
			return nil, err
		}
		docs = append(docs, pdocs...)
	}

	//read last partial partition
	rdocs, err := i.kv.ReadRange(ctx, table, rk.Partition(), nil, rk, 0)
	if err != nil {
		return nil, err
	}

	docs = append(docs, rdocs...)

	return docs, nil
}

// ReadIndex returns index entries corresponding to the requested key range
// If the maxKey is not provides then it returns entries equal to the minKey.
// It returns the range of keys where key >= minKey and key < maxKey
func (i *Index) ReadIndex(ctx context.Context, table string, minKey []byte, maxKey []byte) ([]*api.MicroShardKey, error) {
	var err error
	var docs []kv.Doc
	var rk types.Key

	if minKey == nil {
		err = fmt.Errorf("lower bound of the range should be provided")
		ulog.E(err)
		return nil, err
	}

	lk := types.NewBinaryKey(minKey, partitionBits)

	if maxKey == nil {
		docs, err = i.readOne(ctx, table, lk)
	} else {
		rk = types.NewBinaryKey(maxKey, partitionBits)
		docs, err = i.readRange(ctx, table, lk, rk)
	}

	if ulog.E(err) {
		return nil, err
	}

	res := make([]*api.MicroShardKey, 0, len(docs))
	var prev *api.MicroShardKey
	for _, v := range docs {
		var m api.MicroShardKey
		if err := json.Unmarshal(v.Value, &m); ulog.E(err) {
			return nil, err
		}
		log.Debug().Interface("map", &m).Msg("ReadIndex")
		//filter second entry for the same shard
		if prev != nil && prev.FileId == m.FileId &&
			bytes.Compare(prev.MaxKey, m.MaxKey) == 0 &&
			bytes.Compare(prev.MinKey, m.MinKey) == 0 &&
			prev.Timestamp == m.Timestamp {
			continue
		}
		if len(docs) == 1 && (maxKey == nil || bytes.Compare(lk.Partition(), rk.Partition()) == 0) && m.GetMinKey() != nil {
			continue // do not include single out of range shard
		}
		prev = &m
		res = append(res, &m)
	}

	return res, nil
}

type Metadata struct {
	MicroShard *api.MicroShardKey `json:"micro_shard,omitempty"`
}

type TigrisDBMetadata struct {
	Metadata Metadata
}

type TigrisDBObject struct {
	TigrisDB TigrisDBMetadata `json:"_tigrisdb"`
}

func (i *Index) PatchPrimaryIndex(ctx context.Context, table string, entries []*api.PatchIndexEntry) error {
	for _, v := range entries {
		m := TigrisDBObject{
			TigrisDB: TigrisDBMetadata{
				Metadata: Metadata{
					MicroShard: v.Value,
				},
			},
		}
		data, err := json.Marshal(&m)
		if ulog.E(err) {
			return err
		}
		if err := i.kv.Replace(ctx, table, types.NewUserKey(v.GetPrimaryKey(), v.GetPartitionKey()), data); err != nil {
			return err
		}
	}
	return nil
}

type indexKey struct {
	key       types.Key
	timestamp int64
	fileID    string
	offset    uint64
	encoded   []byte
}

func NewIndexKey(key []byte, ts int64, fileID string, offset uint64) types.Key {
	return &indexKey{key: types.NewBinaryKey(key, partitionBits), timestamp: ts, fileID: fileID, offset: offset, encoded: EncodeIndexKey(key, ts, fileID, offset)}
}

func EncodeIndexKey(key []byte, ts int64, fileID string, offset uint64) []byte {
	enc := make([]byte, 0, len(key)+2+8+8+len(fileID))
	enc = types.EncodeBinaryKey(key, enc)
	enc = types.EncodeIntKey(uint64(ts), enc)
	enc = types.EncodeIntKey(offset, enc)
	enc = types.EncodeBinaryKey([]byte(fileID), enc)
	return enc
}

func (i *indexKey) Partition() []byte {
	return i.key.Partition()
}

func (i *indexKey) Primary() []byte {
	return i.encoded
}

func (i *indexKey) String() string {
	return fmt.Sprintf("%s %d %d %s", i.key.String(), i.timestamp, i.offset, i.fileID)
}


