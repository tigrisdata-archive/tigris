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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/iancoleman/strcase"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/util"
)

var (
	ErrUnsupportedFormat = fmt.Errorf("unsupported format. supported formats are: JSON, TypeScripts, Go, Java")
	ErrEmptyObjectName   = fmt.Errorf("object name should be non-zero length")

	plural = pluralize.NewClient()
)

// Supported data types
// See translateType for Golang to JSON schema translation rules.
const (
	typeInteger = "integer"
	typeString  = "string"
	typeBoolean = "boolean"
	typeNumber  = "number"
	typeArray   = "array"
	typeObject  = "object"
)

// Supported subtypes.
const (
	formatInt32    = "int32"
	formatByte     = "byte"
	formatDateTime = "date-time"
	formatUUID     = "uuid"
)

// TODO: This is copy from the Go client schema package, it cannot be imported due to proto file conflict
// TODO: These structures need to be unified with the server schema package

// Field represents JSON schema object.
type Field struct {
	Type   string            `json:"type,omitempty"`
	Format string            `json:"format,omitempty"`
	Tags   []string          `json:"tags,omitempty"`
	Desc   string            `json:"description,omitempty"`
	Fields map[string]*Field `json:"properties,omitempty"`
	Items  *Field            `json:"items,omitempty"`

	Default      any  `json:"default,omitempty"`
	MaxLength    int  `json:"maxLength,omitempty"`
	CreatedAt    bool `json:"createdAt,omitempty"`
	UpdatedAt    bool `json:"updatedAt,omitempty"`
	AutoGenerate bool `json:"autoGenerate,omitempty"`

	Required []string `json:"required,omitempty"`

	// RequiredTag is used during schema building only
	RequiredTag bool `json:"-"`
}

// Schema is top level JSON schema object.
type Schema struct {
	Name   string            `json:"title,omitempty"`
	Desc   string            `json:"description,omitempty"`
	Fields map[string]*Field `json:"properties,omitempty"`

	PrimaryKey []string `json:"primary_key,omitempty"`
	Required   []string `json:"required,omitempty"`

	CollectionType string `json:"collection_type,omitempty"`
}

type JSONToLangType interface {
	GetType(string, string) (string, error)
	GetObjectTemplate() string
}

type Collection struct {
	Name      string
	NameDecap string
	JSON      string
}

type FieldGen struct {
	Type      string
	TypeDecap string

	Name       string
	NameDecap  string
	NameSnake  string
	NameJSON   string
	NamePlural string
	JSONCap    string

	IsArray  bool
	IsObject bool

	AutoGenerate    bool
	PrimaryKeyIdx   int
	ArrayDimensions int

	Default                any
	DefaultStr             string
	DefaultStrSingleQuotes string

	MaxLength int
	UpdatedAt bool
	CreatedAt bool
	Required  bool

	Description string
}

type Object struct {
	Name      string
	NameDecap string
	NameSnake string
	NameJSON  string

	NamePlural string

	Description string

	Nested bool

	Fields []FieldGen
}

func genField(w io.Writer, n string, v *Field, pk []string, required bool, c JSONToLangType,
	hasTime *bool, hasUUID *bool,
) (*FieldGen, error) {
	var err error

	f := FieldGen{AutoGenerate: v.AutoGenerate}

	f.NameJSON = n
	f.JSONCap = strings.ToUpper(n[0:1]) + n[1:]
	f.NamePlural = plural.Plural(n)
	f.Name = strcase.ToCamel(n)
	f.UpdatedAt = v.UpdatedAt
	f.CreatedAt = v.CreatedAt
	f.MaxLength = v.MaxLength
	f.Required = required

	f.Default = v.Default
	if s, ok := f.Default.(string); ok {
		f.DefaultStr = fmt.Sprintf(`%q`, s)
		f.DefaultStrSingleQuotes = fmt.Sprintf(`'%s'`, strings.ReplaceAll(s, "'", "\\\\'"))
	}

	if v.Type == typeArray {
		f.Name = plural.Plural(f.Name)
	}

	f.NameDecap = strings.ToLower(f.Name[0:1]) + f.Name[1:]
	f.NameSnake = strcase.ToSnake(n)
	f.Description = v.Desc

	for v.Type == typeArray {
		v = v.Items
		f.ArrayDimensions++
	}

	f.IsArray = f.ArrayDimensions > 0

	if v.Type == typeObject {
		if err := genSchema(w, n, v.Desc, v.Fields, nil, v.Required, c, hasTime, hasUUID); err != nil {
			return nil, err
		}

		f.Type = plural.Singular(f.Name)
		f.TypeDecap = strings.ToLower(f.Type[0:1]) + f.Type[1:]
		f.IsObject = true
	} else if f.Type, err = c.GetType(v.Type, v.Format); err != nil {
		return nil, err
	}

	if v.Format == formatUUID {
		*hasUUID = true
	}

	if v.Format == formatDateTime {
		*hasTime = true
	}

	for k1, v1 := range pk {
		if v1 == f.NameJSON {
			f.PrimaryKeyIdx = k1 + 1
		}
	}

	return &f, nil
}

func genSchema(w io.Writer, name string, desc string, field map[string]*Field,
	pk []string, required []string, c JSONToLangType, hasTime *bool, hasUUID *bool,
) error {
	var obj Object

	if len(name) == 0 {
		return ErrEmptyObjectName
	}

	obj.NamePlural = plural.Plural(name)
	obj.NameJSON = name
	name = pluralize.NewClient().Singular(name)
	obj.Name = strcase.ToCamel(name)
	obj.NameDecap = strings.ToLower(obj.Name[0:1]) + obj.Name[1:]
	obj.NameSnake = strcase.ToSnake(obj.Name)
	obj.Description = desc
	obj.Nested = pk == nil

	names := make(sort.StringSlice, 0, len(field))
	for n := range field {
		names = append(names, n)
	}

	sort.Sort(names)

	reqPtr := 0

	for _, n := range names {
		v := field[n]

		if len(n) == 0 {
			return ErrEmptyObjectName
		}

		// TODO: We assume required array is sorted, need fix to not depend on it.
		req := false
		if reqPtr < len(required) {
			if required[reqPtr] == n {
				reqPtr++
				req = true
			}
		}

		f, err := genField(w, n, v, pk, req, c, hasTime, hasUUID)
		if err != nil {
			return err
		}

		obj.Fields = append(obj.Fields, *f)
	}

	if err := util.ExecTemplate(w, c.GetObjectTemplate(), obj); err != nil {
		return err
	}

	return nil
}

func genCollectionSchema(w io.Writer, rawSchema []byte, c JSONToLangType, hasTime *bool, hasUUID *bool) error {
	var sch Schema

	if err := jsoniter.Unmarshal(rawSchema, &sch); err != nil {
		return err
	}

	if err := genSchema(w, sch.Name, sch.Desc, sch.Fields, sch.PrimaryKey, sch.Required, c, hasTime, hasUUID); err != nil {
		return err
	}

	return nil
}

func getGenerator(lang string) (JSONToLangType, error) {
	var genType JSONToLangType

	switch l := strings.ToLower(lang); l {
	case "go", "golang":
		genType = &JSONToGo{}
	case "ts", "typescript":
		genType = &JSONToTypeScript{}
	case "java":
		genType = &JSONToJava{}
	default:
		return nil, ErrUnsupportedFormat
	}

	return genType, nil
}

func GenCollectionSchema(jsonSchema []byte, lang string) ([]byte, error) {
	genType, err := getGenerator(lang)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	w := bufio.NewWriter(&buf)

	var hasTime, hasUUID bool
	if err := genCollectionSchema(w, jsonSchema, genType, &hasTime, &hasUUID); err != nil {
		return nil, err
	}

	if err = w.Flush(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
