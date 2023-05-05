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

package api

import (
	"strings"

	jsoniter "github.com/json-iterator/go"
)

// UnmarshalJSON for SetRequest.
func (x *SetRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v any

		switch strings.ToLower(key) {
		case "project":
			v = &x.Project
		case "name":
			v = &x.Name
		case "key":
			v = &x.Key
		case "value":
			var doc jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &doc); err != nil {
				return err
			}
			x.Value = doc
			continue
		case "ex":
			v = &x.Ex
		case "nx":
			v = &x.Nx
		case "px":
			v = &x.Px
		default:
			continue
		}
		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalJSON for SetRequest.
func (x *GetSetRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v any

		switch strings.ToLower(key) {
		case "project":
			v = &x.Project
		case "name":
			v = &x.Name
		case "key":
			v = &x.Key
		case "value":
			var doc jsoniter.RawMessage
			if err := jsoniter.Unmarshal(value, &doc); err != nil {
				return err
			}
			x.Value = doc
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

// UnmarshalJSON for DelRequest.
func (x *DelRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v any
		switch strings.ToLower(key) {
		case "project":
			v = &x.Project
		case "name":
			v = &x.Name
		case "key":
			v = &x.Key
		default:
			continue
		}
		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalJSON for KeysRequest.
func (x *KeysRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		var v any
		switch strings.ToLower(key) {
		case "project":
			v = &x.Project
		case "name":
			v = &x.Name
		case "cursor":
			v = &x.Cursor
		case "count":
			v = &x.Count
		case "pattern":
			v = &x.Pattern
		default:
			continue
		}
		if err := jsoniter.Unmarshal(value, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *GetSetResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Status   string              `json:"status,omitempty"`
		Message  string              `json:"message,omitempty"`
		OldValue jsoniter.RawMessage `json:"old_value,omitempty"`
	}{
		Status:   x.GetStatus(),
		Message:  x.GetMessage(),
		OldValue: x.GetOldValue(),
	}
	return jsoniter.Marshal(resp)
}

func (x *GetResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Value jsoniter.RawMessage `json:"value,omitempty"`
	}{
		Value: x.GetValue(),
	}
	return jsoniter.Marshal(resp)
}

func (x *KeysResponse) MarshalJSON() ([]byte, error) {
	resp := struct {
		Keys   []string `json:"keys"`
		Cursor uint64   `json:"cursor"`
	}{
		Keys:   x.GetKeys(),
		Cursor: x.GetCursor(),
	}
	return jsoniter.Marshal(resp)
}
