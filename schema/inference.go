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

package schema

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	schema "github.com/tigrisdata/tigris/schema/lang"
)

// Supported subtypes.
var (
	ErrIncompatibleSchema = fmt.Errorf("error incompatible schema")
	ErrExpectedString     = fmt.Errorf("expected string type")
	ErrExpectedNumber     = fmt.Errorf("expected json.Number")
	ErrUnsupportedType    = fmt.Errorf("unsupported type")
)

func parseDateTime(s string) bool {
	if _, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return true
	}

	// FIXME: Support more formats. Need server side support or convert it here in CLI

	return false
}

func translateStringType(v interface{}) (string, string, error) {
	t := reflect.TypeOf(v)

	if t.PkgPath() == "encoding/json" && t.Name() == "Number" {
		n, ok := v.(json.Number)
		if !ok {
			return "", "", ErrExpectedNumber
		}

		if _, err := n.Int64(); err != nil {
			_, err = n.Float64()
			if err != nil {
				return "", "", err
			}

			return jsonSpecDouble, "", nil
		}

		return jsonSpecInt, "", nil
	}

	s, ok := v.(string)
	if !ok {
		return "", "", ErrExpectedString
	}

	if parseDateTime(s) {
		return jsonSpecString, jsonSpecFormatDateTime, nil
	}

	if _, err := uuid.Parse(s); err == nil {
		return jsonSpecString, jsonSpecFormatUUID, nil
	}

	b := make([]byte, base64.StdEncoding.DecodedLen(len(s)))
	if _, err := base64.StdEncoding.Decode(b, []byte(s)); err == nil {
		return jsonSpecString, jsonSpecFormatByte, nil
	}

	return jsonSpecString, "", nil
}

func translateType(v interface{}) (string, string, error) {
	t := reflect.TypeOf(v)

	//nolint:golint,exhaustive
	switch t.Kind() {
	case reflect.Bool:
		return jsonSpecBool, "", nil
	case reflect.Float64:
		return jsonSpecDouble, "", nil
	case reflect.String:
		return translateStringType(v)
	case reflect.Slice, reflect.Array:
		return jsonSpecArray, "", nil
	case reflect.Map:
		return jsonSpecObject, "", nil
	default:
		return "", "", errors.Wrapf(ErrUnsupportedType, "name='%s' kind='%s'", t.Name(), t.Kind())
	}
}

func extendedStringType(oldType string, oldFormat string, newType string, newFormat string) (string, string, error) {
	if newFormat == "" {
		switch {
		case oldFormat == jsonSpecFormatByte:
			return newType, newFormat, nil
		case oldFormat == jsonSpecFormatUUID:
			return newType, newFormat, nil
		case oldFormat == jsonSpecFormatDateTime:
			return newType, newFormat, nil
		}
	} else if newFormat == jsonSpecFormatByte && oldFormat == "" {
		return oldType, oldFormat, nil
	}

	return "", "", ErrIncompatibleSchema
}

// this is only matter for initial schema inference, where we have luxury to extend the type
// if not detected properly from the earlier data records
// we extend:
// int -> float
// byte -> string
// time -> string
// uuid => string.
func extendedType(oldType string, oldFormat string, newType string, newFormat string) (string, string, error) {
	if oldType == jsonSpecInt && newType == jsonSpecDouble {
		return newType, newFormat, nil
	}

	if oldType == jsonSpecString && newType == jsonSpecString {
		if t, f, err := extendedStringType(oldType, oldFormat, newType, newFormat); err == nil {
			return t, f, nil
		}
	}

	if newType == oldType && newFormat == oldFormat {
		return newType, newFormat, nil
	}

	log.Debug().Str("oldType", oldType).Str("newType", newType).Msg("incompatible schema")
	log.Debug().Str("oldFormat", oldFormat).Str("newFormat", newFormat).Msg("incompatible schema")

	return "", "", ErrIncompatibleSchema
}

func traverseObject(existingField *schema.Field, newField *schema.Field, values map[string]any) error {
	switch {
	case existingField == nil:
		newField.Fields = make(map[string]*schema.Field)
	case existingField.Type == jsonSpecObject:
		if existingField.Fields == nil {
			newField.Fields = make(map[string]*schema.Field)
		} else {
			newField.Fields = existingField.Fields
		}
	default:
		return ErrIncompatibleSchema
	}

	return traverseFields(newField.Fields, values, nil)
}

func traverseArray(existingField *schema.Field, newField *schema.Field, v any) error {
	for i := 0; i < reflect.ValueOf(v).Len(); i++ {
		t, format, err := translateType(reflect.ValueOf(v).Index(i).Interface())
		if err != nil {
			return err
		}

		if i == 0 {
			switch {
			case existingField == nil:
				newField.Items = &schema.Field{Type: t, Format: format}
			case existingField.Type == jsonSpecArray:
				newField.Items = existingField.Items
			default:
				return ErrIncompatibleSchema
			}
		}

		nt, nf, err := extendedType(newField.Items.Type, newField.Items.Format, t, format)
		if err != nil {
			return err
		}

		newField.Items.Type = nt
		newField.Items.Format = nf

		if t == jsonSpecObject {
			values, _ := reflect.ValueOf(v).Index(i).Interface().(map[string]any)
			if err := traverseObject(newField.Items, newField.Items, values); err != nil {
				return err
			}

			if len(newField.Items.Fields) == 0 {
				newField.Items = nil
			}
		}
	}

	return nil
}

func setAutoGenerate(autoGen []string, name string, field *schema.Field) {
	if autoGen != nil {
		// FIXME: Convert to O(1)
		for i := 0; i < len(autoGen); i++ {
			if autoGen[i] == name {
				field.AutoGenerate = true
			}
		}
	}
}

func traverseFields(sch map[string]*schema.Field, fields map[string]interface{}, autoGen []string) error {
	for k, v := range fields {
		t, format, err := translateType(v)
		if err != nil {
			return err
		}

		f := &schema.Field{Type: t, Format: format}

		switch {
		case t == jsonSpecObject:
			vm, _ := v.(map[string]any)
			if err := traverseObject(sch[k], f, vm); err != nil {
				return err
			}

			if len(f.Fields) == 0 {
				continue
			}
		case t == jsonSpecArray:
			// FIXME: Support multidimensional arrays
			if reflect.ValueOf(v).Len() == 0 {
				continue // empty array does not reflect in the schema
			}

			if err = traverseArray(sch[k], f, v); err != nil {
				return err
			}

			if f.Items == nil {
				continue // empty object
			}
		case sch[k] != nil:
			nt, nf, err := extendedType(sch[k].Type, sch[k].Format, t, format)
			if err != nil {
				return ErrIncompatibleSchema
			}

			f.Type = nt
			f.Format = nf
		}

		setAutoGenerate(autoGen, k, f)

		sch[k] = f
	}

	return nil
}

func docToSchema(sch *schema.Schema, name string, data []byte, pk []string, autoGen []string) error {
	var m map[string]interface{}

	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.UseNumber()

	if err := dec.Decode(&m); err != nil {
		return err
	}

	sch.Name = name
	if pk != nil {
		sch.PrimaryKey = pk
	}

	if sch.Fields == nil {
		sch.Fields = make(map[string]*schema.Field)
	}

	if err := traverseFields(sch.Fields, m, autoGen); err != nil {
		return err
	}

	// Implicit "id" primary key
	f := sch.Fields["id"]
	if sch.PrimaryKey == nil && f != nil && f.Format == jsonSpecFormatUUID {
		f.AutoGenerate = true
		sch.PrimaryKey = []string{"id"}
	}

	return nil
}

func Infer(sch *schema.Schema, name string, docs [][]byte, primaryKey []string, autoGenerate []string,
	depth int,
) error {
	for i := 0; (depth == 0 || i < depth) && i < len(docs); i++ {
		err := docToSchema(sch, name, docs[i], primaryKey, autoGenerate)
		if err != nil {
			return err
		}
	}

	return nil
}
