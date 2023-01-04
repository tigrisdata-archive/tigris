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

package app

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

type Device struct {
	id   string
	conn *websocket.Conn
}

func NewDevice(id string, url url.URL) (*Device, error) {
	conn, resp, err := websocket.DefaultDialer.Dial(url.String(), http.Header{"encoding": []string{"json"}})
	if err != nil {
		log.Err(err).Msg("connecting to ws error")
		return nil, err
	}
	defer resp.Body.Close()

	d := &Device{
		id:   id,
		conn: conn,
	}

	go d.StartReceiving()

	return d, nil
}

func (d *Device) StartReceiving() {
	for {
		_, message, err := d.conn.ReadMessage()
		if err != nil {
			log.Err(err)
			continue
		}

		var req api.RealTimeMessage
		if err := jsoniter.Unmarshal(message, &req); err != nil {
			log.Err(err).Msg("unmarshalling failed")
			continue
		}

		log.Info().Str("my-id", d.id).Str("event-type", req.EventType.String()).Msg(string(req.GetEvent()))
	}
}

func (d *Device) Attach(channel string) error {
	attachEvent := &api.AttachEvent{
		Channel: channel,
	}
	encAttach, err := jsoniter.Marshal(attachEvent)
	if err != nil {
		return err
	}

	encR, err := jsoniter.Marshal(&api.RealTimeMessage{
		EventType: api.EventType_attach,
		Event:     encAttach,
	})
	if err != nil {
		return err
	}

	return d.conn.WriteMessage(websocket.TextMessage, encR)
}

func (d *Device) DeAttach(channel string) error {
	detachEvent := &api.DetachEvent{
		Channel: channel,
	}
	encDetach, err := jsoniter.Marshal(detachEvent)
	if err != nil {
		return err
	}

	encR, err := jsoniter.Marshal(&api.RealTimeMessage{
		EventType: api.EventType_detach,
		Event:     encDetach,
	})
	if err != nil {
		return err
	}

	return d.conn.WriteMessage(websocket.TextMessage, encR)
}

func (d *Device) Subscribe(channel string) error {
	subscribeEvent := &api.SubscribeEvent{
		Channel: channel,
	}
	encSubscribe, err := jsoniter.Marshal(subscribeEvent)
	if err != nil {
		return err
	}

	encR, err := jsoniter.Marshal(&api.RealTimeMessage{
		EventType: api.EventType_subscribe,
		Event:     encSubscribe,
	})
	if err != nil {
		return err
	}

	if err = d.conn.WriteMessage(websocket.TextMessage, encR); err != nil {
		return err
	}

	return nil
}

func (d *Device) Publish(channel string, id string) error {
	data := []byte(fmt.Sprintf(`{"device_id": "%s", "msg": "saying hi!", "id": "%s"}`, d.id, id))
	messageEvent := &api.MessageEvent{
		Channel: channel,
		Data:    data,
	}
	encMessage, err := jsoniter.Marshal(messageEvent)
	if err != nil {
		return err
	}
	encR, err := jsoniter.Marshal(&api.RealTimeMessage{
		EventType: api.EventType_message,
		Event:     encMessage,
	})
	if err != nil {
		return err
	}

	if err := d.conn.WriteMessage(websocket.TextMessage, encR); err != nil {
		return err
	}
	return nil
}

func (d *Device) Close() {
	d.conn.Close()
}
