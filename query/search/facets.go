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
