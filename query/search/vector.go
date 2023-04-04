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

package search

import (
	"strconv"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
)

const (
	topKJSTag = "top_k"
)

type VectorSearch struct {
	TopK       int `json:"top_k,omitempty"`
	VectorF    string
	VectorV    []float64
	RawVectorV []byte
}

func UnmarshalVectorSearch(input jsoniter.RawMessage) (VectorSearch, error) {
	if len(input) == 0 {
		return VectorSearch{}, nil
	}

	var err error
	var g VectorSearch
	err = jsonparser.ObjectEach(input, func(k []byte, v []byte, jsonDataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		if string(k) == topKJSTag {
			var val int64
			if val, err = strconv.ParseInt(string(v), 10, 32); err != nil {
				return err
			}
			g.TopK = int(val)
		} else {
			if err = jsoniter.Unmarshal(v, &g.VectorV); err != nil {
				return err
			}
			g.VectorF = string(k)
			g.RawVectorV = v
		}
		return err
	})
	if err != nil {
		return VectorSearch{}, err
	}

	return g, nil
}
