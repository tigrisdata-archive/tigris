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

//nolint:golint,dupl
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
interface Product {
  arrInts: Array<string>;
  bool: boolean;
  byte1: string;
  id: number;
  int64: string;
  // int64WithDesc field description
  int64WithDesc: string;
  name: string;
  price: number;
  time1: string;
  uUID1: string;
}

const productSchema: TigrisSchema<Product> = {
  arrInts: {
    type: TigrisDataTypes.ARRAY,
    items: {
      type: TigrisDataTypes.INT64,
    },
  },
  bool: {
    type: TigrisDataTypes.BOOLEAN,
  },
  byte1: {
    type: TigrisDataTypes.BYTE_STRING,
  },
  id: {
    type: TigrisDataTypes.INT32,
  },
  int64: {
    type: TigrisDataTypes.INT64,
  },
  int64WithDesc: {
    type: TigrisDataTypes.INT64,
  },
  name: {
    type: TigrisDataTypes.STRING,
  },
  price: {
    type: TigrisDataTypes.NUMBER,
  },
  time1: {
    type: TigrisDataTypes.DATE_TIME,
  },
  uUID1: {
    type: TigrisDataTypes.UUID,
  },
};
`,
		},
		{
			"tags", tagsTest, `
// Product type description
export interface Product extends TigrisCollectionType {
  Gen?: number;
  Key: number;
  KeyGenIdx?: number;
  name_gen?: number;
  name_gen_key?: number;
  name_key: number;
  user_name: number;
}

export const productSchema: TigrisSchema<Product> = {
  Gen: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      autoGenerate: true,
    },
  },
  Key: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      order: 1,
    },
  },
  KeyGenIdx: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      order: 2,
      autoGenerate: true,
    },
  },
  name_gen: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      autoGenerate: true,
    },
  },
  name_gen_key: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      order: 4,
      autoGenerate: true,
    },
  },
  name_key: {
    type: TigrisDataTypes.INT32,
    primary_key: {
      order: 3,
    },
  },
  user_name: {
    type: TigrisDataTypes.INT32,
  },
};
`,
		},
		{
			"object", objectTest, `
interface SubArrayNested {
  field_3: number;
}

const subArrayNestedSchema: TigrisSchema<SubArrayNested> = {
  field_3: {
    type: TigrisDataTypes.INT32,
  },
};

interface SubObjectNested {
  field_3: number;
}

const subObjectNestedSchema: TigrisSchema<SubObjectNested> = {
  field_3: {
    type: TigrisDataTypes.INT32,
  },
};

interface SubArray {
  field_3: number;
  subArrayNesteds: Array<SubArrayNested>;
  subObjectNested: SubObjectNested;
}

const subArraySchema: TigrisSchema<SubArray> = {
  field_3: {
    type: TigrisDataTypes.INT32,
  },
  subArrayNesteds: {
    type: TigrisDataTypes.ARRAY,
    items: {
      type: subArrayNestedSchema,
    },
  },
  subObjectNested: {
    type: subObjectNestedSchema,
  },
};

// Subtype sub type description
interface Subtype {
  id2: number;
}

const subtypeSchema: TigrisSchema<Subtype> = {
  id2: {
    type: TigrisDataTypes.INT32,
  },
};

export interface Product extends TigrisCollectionType {
  subArrays: Array<SubArray>;
  // subtype sub type description
  subtype: Subtype;
}

export const productSchema: TigrisSchema<Product> = {
  subArrays: {
    type: TigrisDataTypes.ARRAY,
    items: {
      type: subArraySchema,
    },
  },
  subtype: {
    type: subtypeSchema,
  },
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
