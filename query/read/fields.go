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

package read

import (
	"fmt"
	"strconv"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/query/aggregation"
	"github.com/tigrisdata/tigrisdb/query/expression"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/grpc/codes"
)

func BuildFields(reqFields jsoniter.RawMessage) (*FieldFactory, error) {
	var factory = &FieldFactory{}

	if len(reqFields) == 0 {
		return factory, nil
	}

	factory.Include = make(map[string]Field)
	factory.Exclude = make(map[string]Field)

	var err error
	err = jsonparser.ObjectEach(reqFields, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		switch dataType {
		case jsonparser.Boolean:
			// simple field
			var include bool
			include, err = strconv.ParseBool(string(value))
			if err != nil {
				return err
			}

			factory.addField(&SimpleField{
				Name: string(key),
				Incl: include,
			})
		case jsonparser.Number:
			// simple field
			var include int64
			include, err = strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				return err
			}

			factory.addField(&SimpleField{
				Name: string(key),
				Incl: include == 1,
			})
		case jsonparser.Object:
			var expr expression.Expr
			expr, err = aggregation.Unmarshal(value)
			if err != nil {
				return err
			}
			factory.addField(NewExprField(string(key), expr))
		default:
			return api.Errorf(codes.InvalidArgument, "only boolean/integer is supported as value")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return factory, nil
}

type FieldFactory struct {
	Exclude       map[string]Field
	Include       map[string]Field
	FetchedValues map[string]*JSONObject
}

func (factory *FieldFactory) addField(f Field) {
	if !f.Include() {
		factory.Exclude[f.Alias()] = f
		return
	}

	factory.Include[f.Alias()] = f
}

func (factory *FieldFactory) Apply(document []byte) ([]byte, error) {
	if len(factory.Include) == 0 && len(factory.Exclude) == 0 {
		// need to return everything
		return document, nil
	}

	factory.FetchedValues = make(map[string]*JSONObject)
	var err error
	// first extract all fields that may be useful
	err = jsonparser.ObjectEach(document, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		// a, b, c, d
		// a:1, b:0
		// a:1, b:1
		// a:0
		// nil
		if _, ok := factory.Exclude[string(key)]; ok {
			return nil
		}

		factory.FetchedValues[string(key)] = &JSONObject{
			Key:      key,
			Value:    value,
			DataType: dataType,
		}

		return nil
	})

	if len(factory.Include) == 0 && len(factory.Exclude) > 0 {
		return factory.applyExcludeOnly()
	}
	return factory.applyIncludeOnly()
}

func (factory *FieldFactory) applyIncludeOnly() ([]byte, error) {
	var err error
	bb := bytebufferpool.Get()
	_, err = bb.WriteString("{")
	if ulog.E(err) {
		return nil, api.Errorf(codes.Internal, err.Error())
	}

	index := 0
	for _, f := range factory.Include {
		newValue, err := f.Apply(factory.FetchedValues)
		if err != nil {
			return nil, err
		}

		if len(newValue) == 0 {
			continue
		}

		if index != 0 {
			_, err = bb.WriteString(",")
			if ulog.E(err) {
				return nil, api.Errorf(codes.Internal, err.Error())
			}
		}

		_, err = bb.Write(f.GetJSONAlias())
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}

		_, err = bb.WriteString(":")
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}

		_, err = bb.Write(newValue)
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}
		index++
	}
	_, err = bb.WriteString("}")
	if ulog.E(err) {
		return nil, api.Errorf(codes.Internal, err.Error())
	}

	return bb.Bytes(), nil
}

func (factory *FieldFactory) applyExcludeOnly() ([]byte, error) {
	var err error
	bb := bytebufferpool.Get()
	_, err = bb.WriteString("{")
	if ulog.E(err) {
		return nil, api.Errorf(codes.Internal, err.Error())
	}

	index := 0
	for key, js := range factory.FetchedValues {
		if _, ok := factory.Exclude[key]; ok {
			continue
		}

		if index != 0 {
			_, err = bb.WriteString(",")
			if ulog.E(err) {
				return nil, api.Errorf(codes.Internal, err.Error())
			}
		}
		_, err = bb.Write(js.GetKey())
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}

		_, err = bb.WriteString(":")
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}

		_, err = bb.Write(js.GetValue())
		if ulog.E(err) {
			return nil, api.Errorf(codes.Internal, err.Error())
		}
		index++
	}
	_, err = bb.WriteString("}")
	if ulog.E(err) {
		return nil, api.Errorf(codes.Internal, err.Error())
	}

	return bb.Bytes(), nil
}

type Field interface {
	Include() bool
	Alias() string
	GetJSONAlias() []byte
	Apply(map[string]*JSONObject) ([]byte, error)
}

type SimpleField struct {
	Name string
	Incl bool
}

func (s *SimpleField) Include() bool {
	return s.Incl
}

func (s *SimpleField) Alias() string {
	return s.Name
}

func (s *SimpleField) GetJSONAlias() []byte {
	return []byte(fmt.Sprintf(`"%s"`, s.Name))
}

func (s *SimpleField) Apply(data map[string]*JSONObject) ([]byte, error) {
	if js, ok := data[s.Name]; ok {
		return js.GetValue(), nil
	}

	return nil, nil
}

type ExprField struct {
	FieldAlias string
	Expr       expression.Expr
}

func NewExprField(alias string, expr expression.Expr) *ExprField {
	return &ExprField{
		FieldAlias: alias,
		Expr:       expr,
	}
}

func (e *ExprField) Include() bool {
	return true
}

func (e *ExprField) GetJSONAlias() []byte {
	return []byte(fmt.Sprintf(`"%s"`, e.FieldAlias))
}

func (e *ExprField) Alias() string {
	return e.FieldAlias
}

func (e *ExprField) Apply(data map[string]*JSONObject) ([]byte, error) {
	return nil, nil
}

type JSONObject struct {
	Key      []byte
	Value    []byte
	DataType jsonparser.ValueType
}

func (j *JSONObject) GetKey() []byte {
	return []byte(fmt.Sprintf(`"%s"`, j.Key))
}

func (j *JSONObject) GetValue() []byte {
	switch j.DataType {
	case jsonparser.String:
		return []byte(fmt.Sprintf(`"%s"`, j.Value))
	}

	return j.Value
}
