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

package realtime

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tigrisdata/tigris/internal"
)

func TestPusher(t *testing.T) {
	/**
	r := cache.NewCache()
	mgr := NewSessionMgr(r)
	session := mgr.CreateDeviceSession(nil, "abc")
	session.chFactory.CreateChannel(context.TODO(), "test")
	streamPusher, err := NewDevicePusher(session, &api.SubscribeEvent{
		Channel: "test",
	})
	require.NoError(t, err)

	streamPusher.StartPushing()

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Second)
		require.NoError(t, streamPusher.channel.Publish(context.TODO(), getEvent()))
	}*/
}

func getEvent() *internal.StreamData {
	payload := []byte(fmt.Sprintf(`{"key": %s}`, randStringRunes(64)))
	return &internal.StreamData{
		RawData: payload,
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
