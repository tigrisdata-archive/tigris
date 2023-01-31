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

//nolint:dupl
package schema

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen
func TestTypeScriptSchemaGenerator(t *testing.T) {
	cases := []struct {
		name string
		in   string
		exp  string
	}{
		{
			"types", typesTest, `
export class Product {
  @Field({ elements: TigrisDataTypes.INT64 })
  arrInts: Array<string>;

  @Field()
  bool: boolean;

  @Field(TigrisDataTypes.BYTE_STRING)
  byte1: string;

  @Field(TigrisDataTypes.INT32)
  id: number;

  @Field(TigrisDataTypes.INT64)
  int64: string;

  // field description
  @Field(TigrisDataTypes.INT64)
  int64WithDesc: string;

  @Field()
  name: string;

  @Field()
  price: number;

  @Field(TigrisDataTypes.DATE_TIME)
  time1: Date;

  @Field({ elements: TigrisDataTypes.INT64, depth: 2 })
  twoDArr: Array<Array<string>>;

  @Field(TigrisDataTypes.UUID)
  uUID1: string;
};
`,
		},
		{
			"tags", tagsTest, `
// type description
@TigrisCollection("products")
export class Product {
  @Field(TigrisDataTypes.INT32, { autoGenerate: true })
  Gen?: number;

  @PrimaryKey(TigrisDataTypes.INT32, { order: 1 })
  Key: number;

  @PrimaryKey(TigrisDataTypes.INT32, { order: 2, autoGenerate: true })
  KeyGenIdx?: number;

  @Field({ default: Generated.CUID })
  def_val_cuid: string;

  @Field(TigrisDataTypes.DATE_TIME, { default: Generated.NOW })
  def_val_date: Date;

  @Field(TigrisDataTypes.DATE_TIME, { default: "2022-12-01T21:21:21.409Z" })
  def_val_date_const: Date;

  @Field(TigrisDataTypes.INT64, { default: 32 })
  def_val_int: string;

  @Field({ default: "str1" })
  def_val_str: string;

  @Field({ default: "st'r1" })
  def_val_str_q: string;

  @Field(TigrisDataTypes.UUID, { default: Generated.UUID })
  def_val_uuid: string;

  @Field({ maxLength: 11 })
  max_len_str: string;

  @Field({ maxLength: 11 })
  max_len_str_req: string;

  @Field(TigrisDataTypes.INT32, { autoGenerate: true })
  name_gen?: number;

  @PrimaryKey(TigrisDataTypes.INT32, { order: 4, autoGenerate: true })
  name_gen_key?: number;

  @PrimaryKey(TigrisDataTypes.INT32, { order: 3 })
  name_key: number;

  @Field(TigrisDataTypes.INT32)
  req_field: number;

  @Field(TigrisDataTypes.DATE_TIME, { default: Generated.NOW, timestamp: "updatedAt", timestamp: "createdAt" })
  time_f: Date;

  @Field(TigrisDataTypes.INT32)
  user_name: number;
};
`,
		},
		{
			"object", objectTest, `
export class SubArrayNested {
  @Field(TigrisDataTypes.INT32)
  field_3: number;
};

export class SubObjectNested {
  @Field(TigrisDataTypes.INT32)
  field_3: number;
};

export class SubArray {
  @Field(TigrisDataTypes.INT32)
  field_3: number;

  @Field({ elements: SubArrayNested })
  subArrayNesteds: Array<SubArrayNested>;

  @Field()
  subObjectNested: SubObjectNested;
};

// sub type description
export class Subtype {
  @Field(TigrisDataTypes.INT32)
  id2: number;
};

@TigrisCollection("products")
export class Product {
  @Field({ elements: SubArray })
  subArrays: Array<SubArray>;

  // sub type description
  @Field()
  subtype: Subtype;
};
`,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			w := bufio.NewWriter(&buf)
			var hasTime, hasUUID bool
			err := genCollectionSchema(w, []byte(v.in), &JSONToTypeScript{}, &hasTime, &hasUUID)
			require.NoError(t, err)
			_ = w.Flush()
			assert.Equal(t, v.exp, buf.String())
		})
	}
}
