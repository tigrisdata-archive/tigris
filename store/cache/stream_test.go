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

package cache

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
)

func TestStream(t *testing.T) {
	ctx := context.TODO()
	r := NewCache(config.GetTestCacheConfig())

	t.Run("add_read", func(t *testing.T) {
		stream, err := r.CreateOrGetStream(context.TODO(), "test")
		require.NoError(t, err)
		defer func() {
			_ = stream.Delete(ctx)
		}()

		rawI := []byte("hello")
		id, err := stream.Add(ctx, internal.NewStreamData(rawI))
		require.NotEmpty(t, id)
		require.NoError(t, err)

		messages, exists, err := stream.Read(ctx, "0")
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, 1, len(messages.XStream.Messages))

		rawO, err := messages.Decode(messages.XStream.Messages[0])
		require.NoError(t, err)
		require.Equal(t, rawI, rawO.RawData)
	})
	t.Run("consumer_groups", func(t *testing.T) {
		stream, err := r.CreateOrGetStream(context.TODO(), "test")
		require.NoError(t, err)
		defer func() {
			_ = stream.Delete(ctx)
		}()

		require.NoError(t, stream.CreateConsumerGroup(ctx, "first", "0"))
		require.NoError(t, stream.CreateConsumerGroup(ctx, "second", "0"))

		groups, err := stream.GetConsumerGroups(ctx)
		require.NoError(t, err)
		require.Len(t, groups, 3)
		require.Equal(t, DefaultGroup, groups[0].Name)
		require.Equal(t, "first", groups[1].Name)
		require.Equal(t, "second", groups[2].Name)
	})
}

func TestBenchmarkingStreams(t *testing.T) {
	r := NewCache(config.GetTestCacheConfig())
	stream, err := r.CreateOrGetStream(context.TODO(), "test")
	require.NoError(t, err)
	defer func() {
		_ = stream.Delete(context.TODO())
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			start := time.Now()
			_, exists, _ := stream.Read(context.TODO(), "$")
			t.Logf("time taken %v", time.Since(start))
			if !exists {
				return
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		_, err := stream.Add(context.TODO(), getEvent())
		require.NoError(t, err)
	}

	wg.Wait()
}

func getEvent() *internal.StreamData {
	return internal.NewStreamData([]byte(fmt.Sprintf(`{"key": %s}`, RandStringRunes(64))))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		randV, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letterRunes))))
		b[i] = letterRunes[randV.Int64()]
	}
	return string(b)
}
