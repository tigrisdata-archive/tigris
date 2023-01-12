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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen
func TestGoSchemaGenerator(t *testing.T) {
	cases := []struct {
		name string
		in   string
		exp  string
	}{
		{
			"types", typesTest, `
type Product struct {
	ArrInts []int64 ` + "`" + `json:"arrInts"` + "`" + `
	Bool bool ` + "`" + `json:"bool"` + "`" + `
	Byte1 []byte ` + "`" + `json:"byte1"` + "`" + `
	Id int32 ` + "`" + `json:"id"` + "`" + `
	Int64 int64 ` + "`" + `json:"int64"` + "`" + `
	// Int64WithDesc field description
	Int64WithDesc int64 ` + "`" + `json:"int64WithDesc"` + "`" + `
	Name string ` + "`" + `json:"name"` + "`" + `
	Price float64 ` + "`" + `json:"price"` + "`" + `
	Time1 time.Time ` + "`" + `json:"time1"` + "`" + `
	TwoDArrs []int64 ` + "`" + `json:"twoDArr"` + "`" + `
	UUID1 uuid.UUID ` + "`" + `json:"uUID1"` + "`" + `
}
`,
		},
		{
			"tags", tagsTest, `
// Product type description
type Product struct {
	Gen int32 ` + "`" + `tigris:"autoGenerate"` + "`" + `
	Key int32 ` + "`" + `tigris:"primaryKey:1"` + "`" + `
	KeyGenIdx int32 ` + "`" + `tigris:"primaryKey:2,autoGenerate"` + "`" + `
	DefValCuid string ` + "`" + `json:"def_val_cuid"` + "`" + `
	DefValDate time.Time ` + "`" + `json:"def_val_date"` + "`" + `
	DefValDateConst time.Time ` + "`" + `json:"def_val_date_const"` + "`" + `
	DefValInt int64 ` + "`" + `json:"def_val_int"` + "`" + `
	DefValStr string ` + "`" + `json:"def_val_str"` + "`" + `
	DefValUuid uuid.UUID ` + "`" + `json:"def_val_uuid"` + "`" + `
	MaxLenStr string ` + "`" + `json:"max_len_str"` + "`" + `
	NameGen int32 ` + "`" + `json:"name_gen" tigris:"autoGenerate"` + "`" + `
	NameGenKey int32 ` + "`" + `json:"name_gen_key" tigris:"primaryKey:4,autoGenerate"` + "`" + `
	NameKey int32 ` + "`" + `json:"name_key" tigris:"primaryKey:3"` + "`" + `
	UserName int32 ` + "`" + `json:"user_name"` + "`" + `
}
`,
		},
		{"object", objectTest, `
type SubArrayNested struct {
	Field3 int32 ` + "`" + `json:"field_3"` + "`" + `
}

type SubObjectNested struct {
	Field3 int32 ` + "`" + `json:"field_3"` + "`" + `
}

type SubArray struct {
	Field3 int32 ` + "`" + `json:"field_3"` + "`" + `
	SubArrayNesteds []SubArrayNested ` + "`" + `json:"subArrayNesteds"` + "`" + `
	SubObjectNested SubObjectNested ` + "`" + `json:"subObjectNested"` + "`" + `
}

// Subtype sub type description
type Subtype struct {
	Id2 int32 ` + "`" + `json:"id2"` + "`" + `
}

type Product struct {
	SubArrays []SubArray ` + "`" + `json:"subArrays"` + "`" + `
	// Subtype sub type description
	Subtype Subtype ` + "`" + `json:"subtype"` + "`" + `
}
`},
		{
			"no_tag", noGoTagSchema, `
type Product struct {
	Name string
}
`,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			w := bufio.NewWriter(&buf)
			var hasTime, hasUUID bool
			err := genCollectionSchema(w, []byte(v.in), &JSONToGo{}, &hasTime, &hasUUID)
			require.NoError(t, err)
			_ = w.Flush()
			assert.Equal(t, v.exp, buf.String())
		})
	}
}
