// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
)

const (
	defaultFacetSize = 10
)

type Facets struct {
	Fields []FacetField
}

type FacetField struct {
	Name string
	Type string
	Size int
}

func NewFacetField(name string, value jsoniter.RawMessage) (FacetField, error) {
	type facetValue struct {
		Type string
		Size int
	}

	var v facetValue
	if err := jsoniter.Unmarshal(value, &v); err != nil {
		return FacetField{}, err
	}
	if v.Size == 0 {
		v.Size = defaultFacetSize
	}

	return FacetField{
		Name: name,
		Type: v.Type,
		Size: v.Size,
	}, nil
}

func UnmarshalFacet(input jsoniter.RawMessage) (Facets, error) {
	var facets = Facets{}
	var err error
	if len(input) == 0 {
		return facets, nil
	}

	err = jsonparser.ObjectEach(input, func(k []byte, v []byte, jsonDataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		var facetField FacetField
		if facetField, err = NewFacetField(string(k), v); err != nil {
			return err
		}

		facets.Fields = append(facets.Fields, facetField)
		return nil
	})

	return facets, err
}
