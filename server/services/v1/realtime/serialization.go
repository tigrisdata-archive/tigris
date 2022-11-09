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
	"bytes"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/proto"
)

type wsEncodingType int8

const (
	msgpackEncoding wsEncodingType = 1
	jsonEncoding    wsEncodingType = 2
)

const (
	PresenceChannelData = "presence"
	MessageChannelData  = "message"
)

var bh codec.BincHandle

type StreamMessageMD struct {
	ClientId string
	SocketId string
	// DataType is the type of data it is storing for example "presence" or "message" data
	DataType string
	// EventName is the named identifier of this message like in case of presence "enter"/"left", etc
	EventName string
}

func NewStreamMessageMD(dataType string, clientId string, socketId string, eventName string) *StreamMessageMD {
	return &StreamMessageMD{
		DataType:  dataType,
		ClientId:  clientId,
		SocketId:  socketId,
		EventName: eventName,
	}
}

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

func NewPresenceData(clientId string, socketId string, eventName string, msg *api.MessageEvent) (*internal.StreamData, error) {
	return newStreamData(PresenceChannelData, clientId, socketId, eventName, msg.Data)
}

func NewEventDataFromMessage(clientId string, socketId string, eventName string, msg *api.Message) (*internal.StreamData, error) {
	return newStreamData(MessageChannelData, clientId, socketId, eventName, msg.Data)
}

func NewMessageData(clientId string, socketId string, eventName string, msg *api.MessageEvent) (*internal.StreamData, error) {
	return newStreamData(MessageChannelData, clientId, socketId, eventName, msg.Data)
}

func newStreamData(dataType string, clientId string, socketId string, eventName string, rawData []byte) (*internal.StreamData, error) {
	md := NewStreamMessageMD(dataType, clientId, socketId, eventName)
	enc, err := EncodeStreamMD(md)
	if err != nil {
		return nil, err
	}

	return internal.NewStreamData(enc, rawData), nil
}

func EncodeRealtime(encodingType wsEncodingType, msg *api.RealTimeMessage) ([]byte, error) {
	switch encodingType {
	case msgpackEncoding:
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &bh)
		if err := enc.Encode(msg); ulog.E(err) {
			return nil, err
		}
		return buf.Bytes(), nil
	case jsonEncoding:
		return jsoniter.Marshal(msg)
	}

	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func EncodeEvent(encodingType wsEncodingType, eventType api.EventType, event proto.Message) ([]byte, error) {
	switch encodingType {
	case msgpackEncoding:
		return EncodeAsMsgPack(eventType, event)
	case jsonEncoding:
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

func DecodeRealtime(encodingType wsEncodingType, message []byte) (*api.RealTimeMessage, error) {
	switch encodingType {
	case msgpackEncoding:
	case jsonEncoding:
		var req *api.RealTimeMessage
		err := jsoniter.Unmarshal(message, &req)
		return req, err
	}
	return nil, fmt.Errorf("unsupported event '%d'", encodingType)
}

func DecodeEvent(encodingType wsEncodingType, eventType api.EventType, message []byte) (proto.Message, error) {
	switch encodingType {
	case msgpackEncoding:
	case jsonEncoding:
		switch eventType {
		case api.EventType_auth:
			var event *api.AuthEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_subscribe:
			var event *api.SubscribeEvent
			err := jsoniter.Unmarshal(message, &event)
			return event, err
		case api.EventType_presence_subscribe:
			var event *api.SubscribeEvent
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
		case api.EventType_presence_message:
			var event *api.Message
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
