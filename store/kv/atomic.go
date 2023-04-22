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
	NumShards             = 128
	AtomicSuffixSeparator = "__A__"
)

// shard is 128 static shards, which is randomly picked on AtomicAdd.
// the adea is to spread the FDB range where the shard belongs to
// prevent bottlenecks.
// This array can be extended.
// On shrink or shard name change the persisted atomic values will be lost.
var shards = [NumShards]string{
	"v27t99NE", "P1NW61m2", "6jDXBKFu", "kGlzr2YZ", "8N31iXRR", "41NCAyEC", "ZJ90gTMg", "WIsa8zCq",
	"CLGKcl5i", "4ZHWtEAe", "3iGtcUpy", "ppYwe1Fi", "TteDP1bw", "dAcG3AI5", "pjvzomB1", "I6XeWQtn",
	"gE4DRqoK", "4jM4TLnE", "gQeTa2Ec", "rWWF3VZB", "YBM6ZB4E", "cJoxa0PW", "4zXkxmDQ", "WgdP4lOO",
	"92SPlusc", "6NTcBua7", "BIA6fWqY", "gGJ15hZl", "UEn2VCBk", "d02XVPU2", "wg7GraYU", "Yy92rvaM",
	"ZRcI7b3A", "hJh9aDzw", "KihwT4uy", "IEZc8Ic9", "Z3LqgCq0", "KOh9gCFF", "XN4vuSdD", "lppjSm39",
	"zvbS9RU8", "Ci6WaGxE", "VDCe1ibQ", "vHCI95EV", "Q71oCbx0", "20Syn9nj", "sjIV7g5i", "ekc1uMV7",
	"yYY6WxTz", "ASUM9e92", "TNGarTZ1", "bAKHsd33", "ea3ASCCj", "biIHW7rR", "0khDWXcr", "55mHHuIW",
	"CsBb5sQ8", "NL0sT4eG", "GsVpHY9r", "PFBDiJh1", "bftO0a6b", "pJSSa8tQ", "wK4dE1Q9", "hMS8tvZ1",
	"NO8JlTXF", "lbCJ4iON", "0LhbRdAs", "0MIPe4io", "lI2SaPkc", "NfNhD0W5", "tDAI0ceM", "aZcm7YD0",
	"3Qies3KR", "0lRQv54p", "wB16FMTA", "aH0FtNPi", "DI8rzxLa", "s0iRRhXF", "krAFA2UH", "RpMe37pr",
	"IKC6aRKe", "A9guqMUq", "v7nMGu7n", "bSKx6v1Y", "1bweYimp", "LG5PYwGF", "04Cqnpq9", "wGvPK3xB",
	"uaFn14Kr", "qsLOZD92", "T2E77luL", "AJM5bpBF", "b6mXdEun", "yRa8Fymz", "HvFZn23A", "2WRTn04W",
	"I6No0Mul", "D0aDhAo7", "Npop1exp", "dZ7Y2Y21", "eI4RaafH", "2CssCr3u", "Qd26If0b", "8ANvmMP0",
	"OFcVe9NQ", "s8gQAXxO", "5Ro1b6FV", "fXDjw5S6", "mucDn1wF", "9R2vQO08", "l9Q0wrbT", "3CMNRxlH",
	"PBrW8Vbc", "3v5OjTCI", "Thk72J6h", "usONXhh5", "lxz0ATU7", "7WJAbBsK", "snql1Fi7", "fuP2GdMh",
	"edp3m6Qb", "Q7J43hw7", "90gn2rZY", "r0nLAtu5", "Q5bnj2VI", "QmenpTx0", "6gImYfts", "kr1TE8OD",
}

type ShardedAtomics interface {
	AtomicAddTx(ctx context.Context, tx Tx, table []byte, key Key, value int64) error
	AtomicReadTx(ctx context.Context, tx Tx, table []byte, key Key) (int64, error)
	AtomicAdd(ctx context.Context, table []byte, key Key, value int64) error
	AtomicRead(ctx context.Context, table []byte, key Key) (int64, error)
}

type shardedAtomics struct {
	kv        TxStore
	numShards int
}

func NewShardedAtomics(kv TxStore) ShardedAtomics {
	return &shardedAtomics{
		numShards: NumShards,
		kv:        kv,
	}
}

// AtomicAddTx applies increment to random shard in the transaction.
func (s *shardedAtomics) AtomicAddTx(ctx context.Context, tx Tx, table []byte, key Key, inc int64) error {
	n := rand.Intn(s.numShards) //nolint:gosec

	key = append(key, AtomicSuffixSeparator)
	key = append(key, shards[n])

	return tx.AtomicAdd(ctx, table, key, inc)
}

// AtomicReadTx calculates the value by summing all the shards in the transaction.
func (s *shardedAtomics) AtomicReadTx(ctx context.Context, tx Tx, table []byte, key Key) (int64, error) {
	it, err := tx.AtomicReadPrefix(ctx, table, key, true)
	if err != nil {
		return 0, err
	}

	var sum int64

	var kv FdbBaseKeyValue[int64]
	for it.Next(&kv) {
		sum += kv.Data
	}

	if it.Err() != nil {
		return 0, it.Err()
	}

	return sum, nil
}

// AtomicAdd applies increment to random shard in the transaction.
func (s *shardedAtomics) AtomicAdd(ctx context.Context, table []byte, key Key, inc int64) error {
	tx, err := s.kv.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.AtomicAddTx(ctx, tx, table, key, inc); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// AtomicRead calculates the value by summing all the shards in the transaction.
func (s *shardedAtomics) AtomicRead(ctx context.Context, table []byte, key Key) (int64, error) {
	tx, err := s.kv.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	return s.AtomicReadTx(ctx, tx, table, key)
}
