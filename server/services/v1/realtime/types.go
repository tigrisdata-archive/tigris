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
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
)

const (
	PresenceChannelData = "presence"
	MessageChannelData  = "message"
)

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

func NewPresenceData(encType internal.UserDataEncType, clientId string, socketId string, eventName string, msg *api.MessageEvent) (*internal.StreamData, error) {
	return newStreamData(PresenceChannelData, encType, clientId, socketId, eventName, msg.Data)
}

func NewMessageData(encType internal.UserDataEncType, clientId string, socketId string, eventName string, msg *api.MessageEvent) (*internal.StreamData, error) {
	return newStreamData(MessageChannelData, encType, clientId, socketId, eventName, msg.Data)
}

func NewEventDataFromMessage(encType internal.UserDataEncType, clientId string, socketId string, eventName string, msg *api.Message) (*internal.StreamData, error) {
	return newStreamData(MessageChannelData, encType, clientId, socketId, eventName, msg.Data)
}

func newStreamData(dataType string, encType internal.UserDataEncType, clientId string, socketId string, eventName string, rawData []byte) (*internal.StreamData, error) {
	md := NewStreamMessageMD(dataType, clientId, socketId, eventName)
	encMD, err := EncodeStreamMD(md)
	if err != nil {
		return nil, err
	}

	return internal.NewStreamData(encType, encMD, rawData), nil
}
