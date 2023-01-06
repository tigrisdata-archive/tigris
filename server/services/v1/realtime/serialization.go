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
	"github.com/tigrisdata/tigris/internal"
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

func EncodeRealtime(encodingType internal.UserDataEncType, msg *api.RealTimeMessage) ([]byte, error) {
	switch encodingType {
	case internal.MsgpackEncoding:
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &msgpackHandle)
		if err := enc.Encode(msg); ulog.E(err) {
			return nil, err
		}
		return buf.Bytes(), nil
	case internal.JsonEncoding:
		return jsoniter.Marshal(msg)
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func EncodeEvent(encodingType internal.UserDataEncType, event proto.Message) ([]byte, error) {
	switch encodingType {
	case internal.MsgpackEncoding:
		return EncodeEventAsMsgPack(event)
	case internal.JsonEncoding:
		return EncodeAsJSON(event)
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func EncodeAsJSON(event proto.Message) ([]byte, error) {
	return jsoniter.Marshal(event)
}

func EncodeEventAsMsgPack(event proto.Message) ([]byte, error) {
	return EncodeAsMsgPack(event)
}

func EncodeAsMsgPack(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &msgpackHandle)
	if err := enc.Encode(data); ulog.E(err) {
		return nil, err
	}
	return buf.Bytes(), nil
}

func JsonByteToMsgPack(data []byte) ([]byte, error) {
	var obj interface{}
	err := jsoniter.Unmarshal(data, &obj)

	if err != nil {
		return nil, err
	}

	return EncodeAsMsgPack(obj)
}

func DecodeRealtime(encodingType internal.UserDataEncType, message []byte) (*api.RealTimeMessage, error) {
	var req *api.RealTimeMessage
	switch encodingType {
	case internal.MsgpackEncoding:
		err := codec.NewDecoderBytes(message, &msgpackHandle).Decode(&req)
		return req, err
	case internal.JsonEncoding:
		err := jsoniter.Unmarshal(message, &req)
		return req, err
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func DecodeEvent(encodingType internal.UserDataEncType, eventType api.EventType, message []byte) (proto.Message, error) {
	var event proto.Message

	switch eventType {
	case api.EventType_auth:
		event = &api.AuthEvent{}
	case api.EventType_heartbeat:
		event = &api.HeartbeatEvent{}
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
	case internal.MsgpackEncoding:
		err := codec.NewDecoderBytes(message, &msgpackHandle).Decode(&event)
		return event, err
	case internal.JsonEncoding:
		err := jsoniter.Unmarshal(message, &event)
		return event, err
	}

	return nil, fmt.Errorf("unsupported encoding '%d'", encodingType)
}

// SanitizeUserData is an optimization so that we can store received raw data and return as-is in case encoding that is
// used during writing is same as during reading. In case encoding changed then this method is doing a conversion. A typical
// case of needing this conversion is when the message is published through websocket and encoding used was msgpack and
// then these messages were read back using HTTP APIs in that case we need to convert from msgpack to JSON.
func SanitizeUserData(toEnc internal.UserDataEncType, data *internal.StreamData) ([]byte, error) {
	if int32(toEnc) == data.Encoding {
		// for websocket communication toEnc should be msgpack by default which is how the data is encoded
		// and stored in cache.
		return data.RawData, nil
	}

	var rawDecoded interface{}
	switch internal.UserDataEncType(data.Encoding) {
	case internal.MsgpackEncoding:
		if err := codec.NewDecoderBytes(data.RawData, &msgpackHandle).Decode(&rawDecoded); err != nil {
			return nil, err
		}
	case internal.JsonEncoding:
		if err := jsoniter.Unmarshal(data.RawData, &rawDecoded); err != nil {
			return nil, err
		}
	}

	switch toEnc {
	case internal.MsgpackEncoding:
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &msgpackHandle)
		if err := enc.Encode(rawDecoded); ulog.E(err) {
			return nil, err
		}
		return buf.Bytes(), nil
	case internal.JsonEncoding:
		return jsoniter.Marshal(rawDecoded)
	}

	return nil, fmt.Errorf("unsupported encoding '%d'", toEnc)
}
