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

package api

import (
	jsoniter "github.com/json-iterator/go"
)

func (x *RealTimeMessage) MarshalJSON() ([]byte, error) {
	resp := struct {
		Event     jsoniter.RawMessage `json:"event,omitempty"`
		EventType EventType           `json:"event_type,omitempty"`
	}{
		Event:     x.Event,
		EventType: x.EventType,
	}
	return jsoniter.Marshal(resp)
}

func (x *AuthEvent) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		if key == "accessToken" {
			x.AccessToken = value
		}
	}

	return nil
}

func (x *UnsubscribeEvent) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		if key == "channel" {
			var channel string
			if err := jsoniter.Unmarshal(value, &channel); err != nil {
				return err
			}
			x.Channel = channel
		}
	}

	return nil
}

func (x *SubscribeEvent) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "channel":
			var channel string
			if err := jsoniter.Unmarshal(value, &channel); err != nil {
				return err
			}
			x.Channel = channel
		case "position":
			var position string
			if err := jsoniter.Unmarshal(value, &position); err != nil {
				return err
			}
			x.Position = position
		}
	}

	return nil
}

func (x *MessageEvent) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "id":
			var id string
			if err := jsoniter.Unmarshal(value, &id); err != nil {
				return err
			}
			x.Id = id
		case "channel":
			var channel string
			if err := jsoniter.Unmarshal(value, &channel); err != nil {
				return err
			}
			x.Channel = channel
		case "name":
			var name string
			if err := jsoniter.Unmarshal(value, &name); err != nil {
				return err
			}
			x.Name = name
		case "data":
			x.Data = value
		}
	}

	return nil
}

func (x *RealTimeMessage) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "event_type":
			var eventType EventType
			if err := jsoniter.Unmarshal(value, &eventType); err != nil {
				return err
			}
			x.EventType = eventType
		case "event":
			x.Event = value
		}
	}

	return nil
}

func (x *Message) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage

	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v interface{}

		switch key {
		case "id":
			v = &x.Id
		case "name":
			v = &x.Name
		case "data":
			// to avoid decoding data
			x.Data = value
			continue
		default:
			continue
		}
		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}
	return nil
}
