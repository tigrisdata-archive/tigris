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

	"github.com/gorilla/websocket"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/store/cache"
)

type DevicePusher struct {
	channel    string
	sessionId  string
	socketId   string
	connection *websocket.Conn
}

func NewDevicePusher(session *Session, channel string) *DevicePusher {
	return &DevicePusher{
		channel:    channel,
		sessionId:  session.id,
		socketId:   session.socketId,
		connection: session.conn,
	}
}

func (pusher *DevicePusher) Watch(events *cache.StreamMessages, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	var processed []string
	for _, m := range events.Messages {
		data, err := events.Decode(m)
		if err != nil {
			continue
		}

		md, err := DecodeStreamMD(data.Metadata)
		if err != nil {
			continue
		}
		processed = append(processed, m.ID)
		if md.ClientId == pusher.sessionId {
			continue
		}
		pusher.sendMessage(m.ID, md, data)
	}

	return processed, nil

}

func (pusher *DevicePusher) sendMessage(msgId string, md *StreamMessageMD, data *internal.StreamData) {
	msgEventBytes := fmt.Sprintf(`{"id": "%s", "name": "%s", "channel": "%s", "timestamp": "%d", "data": "%s"}`,
		msgId, md.EventName, pusher.channel, data.CreatedAt.UnixNano(), data.RawData)

	/**msg := &api.RealTimeMessage{
		SessionId: pusher.id,
		SocketId:  pusher.socketId,
		Event:     []byte(msgEventBytes),
		EventType: api.EventType_message.String(),
	}*/
	var msgBytes string
	if md.DataType == PresenceChannelData {
		msgBytes = fmt.Sprintf(`{"action": "presence", "eventType": "%s", "event": "%s"}`, api.EventType_message.String(), msgEventBytes)
	} else {
		msgBytes = fmt.Sprintf(`{"eventType": "%s", "event": "%s"}`, api.EventType_message.String(), msgEventBytes)
	}

	_ = pusher.connection.WriteMessage(
		websocket.TextMessage,
		[]byte(msgBytes),
	)
}
