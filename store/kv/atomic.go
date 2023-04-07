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
	"math/rand"
)

const (
	DefaultNumShards      = 1024
	AtomicSuffixSeparator = "__A__"
)

type ShardedAtomics interface {
	AtomicAdd(ctx context.Context, table []byte, key Key, value int64) error
	AtomicRead(ctx context.Context, table []byte, key Key) (int64, error)
}

type shardedAtomics struct {
	numShards int
	kv        baseKV
}

func NewShardedAtomics(kv baseKV) ShardedAtomics {
	return &shardedAtomics{
		numShards: DefaultNumShards,
		kv:        kv,
	}
}

// AtomicAdd applies increment to random shard.
func (s *shardedAtomics) AtomicAdd(ctx context.Context, table []byte, key Key, inc int64) error {
	n := rand.Intn(s.numShards) //nolint:gosec

	key = append(key, AtomicSuffixSeparator)
	key = append(key, n)

	return s.kv.AtomicAdd(ctx, table, key, inc)
}

// AtomicRead calculates the value by summing all the shards.
func (s *shardedAtomics) AtomicRead(ctx context.Context, table []byte, key Key) (int64, error) {
	it, err := s.kv.Read(ctx, table, key)
	if err != nil {
		return 0, err
	}

	var sum int64

	var kv baseKeyValue
	for it.Next(&kv) {
		i, err := fdbByteToInt64(kv.Value)
		if err != nil {
			return 0, err
		}

		sum += i
	}

	if it.Err() != nil {
		return 0, it.Err()
	}

	return sum, nil
}
