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

//go:build integration

package server

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/davecgh/go-spew/spew"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"gopkg.in/gavv/httpexpect.v1"
)

func getRealtimeChannelsMethodURL(projectName string, channelName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/realtime/channels/%s/%s", projectName, channelName, methodName)
}

func getRealtimeChannelURL(projectName string, channelName string) string {
	return fmt.Sprintf("/v1/projects/%s/realtime/channels/%s", projectName, channelName)
}

func createChannel(t *testing.T, projectName string, channelName string) *httpexpect.Response {
	// for now creating a dummy record to create a channel
	return expectRealtime(t).POST(getRealtimeChannelsMethodURL(projectName, channelName, "messages")).
		WithJSON(Map{
			"messages": []Doc{
				{"name": "test", "data": Doc{"dummy": "record"}},
			},
		}).
		Expect()
}

func TestGetChannel(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	channelName := "test_channel"
	resp := createChannel(t, project, channelName)
	resp.Status(http.StatusOK)

	// check get channel request
	expectRealtime(t).GET(getRealtimeChannelURL(project, channelName)).
		Expect().Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("channel", "test_channel")
}

func TestMessages(t *testing.T) {
	project := setupTestsOnlyProject(t)
	defer cleanupTests(t, project)

	var messages = make([]Doc, 2)
	messages[0] = Doc{"seq": "first"}
	messages[1] = Doc{"seq": "second"}
	var inputMessages = []Doc{
		{"name": "test", "data": messages[0]},
		{"name": "test", "data": messages[1]},
	}

	channelName := "test_channel_messages"
	expectRealtime(t).POST(getRealtimeChannelsMethodURL(project, channelName, "messages")).
		WithJSON(Map{
			"messages": inputMessages,
		}).
		Expect().
		Status(http.StatusOK)

	// check get channel request
	str := expectRealtime(t).
		GET(getRealtimeChannelsMethodURL(project, channelName, "messages")).
		WithQuery("start", "0").
		Expect().Status(http.StatusOK).
		Body().
		Raw()

	index := 0
	dec := jsoniter.NewDecoder(bytes.NewReader([]byte(str)))
	for dec.More() {
		var mp map[string]jsoniter.RawMessage
		require.NoError(t, dec.Decode(&mp))

		var msg map[string]jsoniter.RawMessage
		if mp["result"] == nil {
			spew.Dump(mp)
		}
		require.NotNil(t, mp["result"])
		require.NoError(t, jsoniter.Unmarshal(mp["result"], &msg))

		var userMsg map[string]jsoniter.RawMessage
		require.NoError(t, jsoniter.Unmarshal(msg["message"], &userMsg))

		jsInput, err := jsoniter.Marshal(messages[index])
		require.NoError(t, err)
		require.JSONEq(t, string(jsInput), string(userMsg["data"]))
		index++
	}
}
