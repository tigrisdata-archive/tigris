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

var msgpackHandle = codec.MsgpackHandle{
	WriteExt: true, // Encodes Byte as binary. See http://ugorji.net/blog/go-codec-primer under Format specific Runtime Configuration
}

func EncodeStreamMD(md *StreamMessageMD) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &msgpackHandle)
	if err := enc.Encode(md); ulog.E(err) {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeStreamMD(enc []byte) (*StreamMessageMD, error) {
	dec := codec.NewDecoderBytes(enc, &msgpackHandle)

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
		enc := codec.NewEncoder(&buf, &msgpackHandle)
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
	enc := codec.NewEncoder(&buf, &msgpackHandle)
	if err := enc.Encode(event); ulog.E(err) {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeRealtime(encodingType WSEncodingType, message []byte) (*api.RealTimeMessage, error) {
	var req *api.RealTimeMessage
	switch encodingType {
	case MsgpackEncoding:
		err := codec.NewDecoderBytes(message, &msgpackHandle).Decode(&req)
		return req, err
	case JsonEncoding:
		err := jsoniter.Unmarshal(message, &req)
		return req, err
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func DecodeEvent(encodingType WSEncodingType, eventType api.EventType, message []byte) (proto.Message, error) {
	var event proto.Message

	switch eventType {
	case api.EventType_auth:
		event = &api.AuthEvent{}
	case api.EventType_subscribe:
		event = &api.SubscribeEvent{}
	case api.EventType_presence:
		event = &api.PresenceEvent{}
	case api.EventType_attach:
		event = &api.AttachEvent{}
	case api.EventType_detach:
		event = &api.DetachEvent{}
	case api.EventType_subscribed:
		event = &api.SubscribedEvent{}
	case api.EventType_message:
		event = &api.MessageEvent{}
	case api.EventType_presence_member:
		event = &api.PresenceMemberEvent{}
	case api.EventType_disconnect:
		event = &api.DisconnectEvent{}
	default:
		return nil, fmt.Errorf("unsupported eventtype '%d'", eventType)
	}

	switch encodingType {
	case MsgpackEncoding:
		err := codec.NewDecoderBytes(message, &msgpackHandle).Decode(&event)
		return event, err
	case JsonEncoding:
		err := jsoniter.Unmarshal(message, &event)
		return event, err
	}

	return nil, fmt.Errorf("unsupported encoding '%d'", encodingType)
}
