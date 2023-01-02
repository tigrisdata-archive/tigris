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

package realtime

import (
	"bytes"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/proto"
)

var bh codec.BincHandle

func EncodeStreamMD(md *StreamMessageMD) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &bh)
	if err := enc.Encode(md); ulog.E(err) {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeStreamMD(enc []byte) (*StreamMessageMD, error) {
	dec := codec.NewDecoderBytes(enc, &bh)

	var v *StreamMessageMD
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func EncodeRealtime(encodingType WSEncodingType, msg *api.RealTimeMessage) ([]byte, error) {
	switch encodingType {
	case MsgpackEncoding:
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &bh)
		if err := enc.Encode(msg); ulog.E(err) {
			return nil, err
		}
		return buf.Bytes(), nil
	case JsonEncoding:
		return jsoniter.Marshal(msg)
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func EncodeEvent(encodingType WSEncodingType, eventType api.EventType, event proto.Message) ([]byte, error) {
	switch encodingType {
	case MsgpackEncoding:
		return EncodeAsMsgPack(eventType, event)
	case JsonEncoding:
		return EncodeAsJSON(eventType, event)
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func EncodeAsJSON(_ api.EventType, event proto.Message) ([]byte, error) {
	return jsoniter.Marshal(event)
}

func EncodeAsMsgPack(eventType api.EventType, event proto.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := buf.WriteByte(byte(eventType)); err != nil {
		return nil, err
	}

	enc := codec.NewEncoder(&buf, &bh)
	if err := enc.Encode(event); ulog.E(err) {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeRealtime(encodingType WSEncodingType, message []byte) (*api.RealTimeMessage, error) {
	switch encodingType {
	case MsgpackEncoding:
	case JsonEncoding:
		var req *api.RealTimeMessage
		err := jsoniter.Unmarshal(message, &req)
		return req, err
	}
	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func DecodeEvent(encodingType WSEncodingType, eventType api.EventType, message []byte) (proto.Message, error) {
	switch encodingType {
	case MsgpackEncoding:
	case JsonEncoding:
		switch eventType {
		case api.EventType_auth:
			var event *api.AuthEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_subscribe:
			var event *api.SubscribeEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_presence:
			var event *api.PresenceEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_subscribed:
			var event *api.SubscribedEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_message:
			var event *api.MessageEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_presence_member:
			var event *api.PresenceMemberEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_disconnect:
			var event *api.DisconnectEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		default:
			return nil, fmt.Errorf("unsupported '%d'", eventType)
		}
	}
	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}
