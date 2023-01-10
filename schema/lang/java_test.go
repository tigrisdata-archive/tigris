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
func TestJavaSchemaGenerator(t *testing.T) {
	cases := []struct {
		name string
		in   string
		exp  string
	}{
		{
			"types", typesTest, `
class Product {
    private long[] arrInts;
    private boolean bool;
    private byte[] byte1;
    private int id;
    private long int64;
    @TigrisField(description = "field description")
    private long int64WithDesc;
    private String name;
    private double price;
    private Date time1;
    private UUID uUID1;

    public long[] getArrInts() {
        return arrInts;
    }

    public void setArrInts(long[] arrInts) {
        this.arrInts = arrInts;
    }

    public boolean isBool() {
        return bool;
    }

    public void setBool(boolean bool) {
        this.bool = bool;
    }

    public byte[] getByte1() {
        return byte1;
    }

    public void setByte1(byte[] byte1) {
        this.byte1 = byte1;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getInt64() {
        return int64;
    }

    public void setInt64(long int64) {
        this.int64 = int64;
    }

    public long getInt64WithDesc() {
        return int64WithDesc;
    }

    public void setInt64WithDesc(long int64WithDesc) {
        this.int64WithDesc = int64WithDesc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Date getTime1() {
        return time1;
    }

    public void setTime1(Date time1) {
        this.time1 = time1;
    }

    public UUID getUUID1() {
        return uUID1;
    }

    public void setUUID1(UUID uUID1) {
        this.uUID1 = uUID1;
    }

    public Product() {};

    public Product(
        long[] arrInts,
        boolean bool,
        byte[] byte1,
        int id,
        long int64,
        long int64WithDesc,
        String name,
        double price,
        Date time1,
        UUID uUID1
    ) {
        this.arrInts = arrInts;
        this.bool = bool;
        this.byte1 = byte1;
        this.id = id;
        this.int64 = int64;
        this.int64WithDesc = int64WithDesc;
        this.name = name;
        this.price = price;
        this.time1 = time1;
        this.uUID1 = uUID1;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Product other = (Product) o;
        return
            Arrays.equals(arrInts, other.arrInts) &&
            bool == other.bool &&
            byte1 == other.byte1 &&
            id == other.id &&
            int64 == other.int64 &&
            int64WithDesc == other.int64WithDesc &&
            name == other.name &&
            price == other.price &&
            time1 == other.time1 &&
            uUID1 == other.uUID1;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            arrInts,
            bool,
            byte1,
            id,
            int64,
            int64WithDesc,
            name,
            price,
            time1,
            uUID1
        );
    }
}
`,
		},
		{
			"tags", tagsTest, `
// Product type description
@com.tigrisdata.db.annotation.TigrisCollection(value = "products")
public class Product implements TigrisCollectionType {
    @TigrisPrimaryKey(autoGenerate = true)
    private int Gen;
    @TigrisPrimaryKey(order = 1)
    private int Key;
    @TigrisPrimaryKey(order = 2, autoGenerate = true)
    private int KeyGenIdx;
    @TigrisPrimaryKey(autoGenerate = true)
    private int name_gen;
    @TigrisPrimaryKey(order = 4, autoGenerate = true)
    private int name_gen_key;
    @TigrisPrimaryKey(order = 3)
    private int name_key;
    private int user_name;

    public int getGen() {
        return Gen;
    }

    public void setGen(int gen) {
        this.Gen = gen;
    }

    public int getKey() {
        return Key;
    }

    public void setKey(int key) {
        this.Key = key;
    }

    public int getKeyGenIdx() {
        return KeyGenIdx;
    }

    public void setKeyGenIdx(int keyGenIdx) {
        this.KeyGenIdx = keyGenIdx;
    }

    public int getName_gen() {
        return name_gen;
    }

    public void setName_gen(int nameGen) {
        this.name_gen = nameGen;
    }

    public int getName_gen_key() {
        return name_gen_key;
    }

    public void setName_gen_key(int nameGenKey) {
        this.name_gen_key = nameGenKey;
    }

    public int getName_key() {
        return name_key;
    }

    public void setName_key(int nameKey) {
        this.name_key = nameKey;
    }

    public int getUser_name() {
        return user_name;
    }

    public void setUser_name(int userName) {
        this.user_name = userName;
    }

    public Product() {};

    public Product(
        int gen,
        int key,
        int keyGenIdx,
        int nameGen,
        int nameGenKey,
        int nameKey,
        int userName
    ) {
        this.Gen = gen;
        this.Key = key;
        this.KeyGenIdx = keyGenIdx;
        this.name_gen = nameGen;
        this.name_gen_key = nameGenKey;
        this.name_key = nameKey;
        this.user_name = userName;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Product other = (Product) o;
        return
            Gen == other.Gen &&
            Key == other.Key &&
            KeyGenIdx == other.KeyGenIdx &&
            name_gen == other.name_gen &&
            name_gen_key == other.name_gen_key &&
            name_key == other.name_key &&
            user_name == other.user_name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Gen,
            Key,
            KeyGenIdx,
            name_gen,
            name_gen_key,
            name_key,
            user_name
        );
    }
}
`,
		},
		{"object", objectTest, `
class SubArrayNested {
    private int field_3;

    public int getField_3() {
        return field_3;
    }

    public void setField_3(int field3) {
        this.field_3 = field3;
    }

    public SubArrayNested() {};

    public SubArrayNested(
        int field3
    ) {
        this.field_3 = field3;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubArrayNested other = (SubArrayNested) o;
        return
            field_3 == other.field_3;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field_3
        );
    }
}

class SubObjectNested {
    private int field_3;

    public int getField_3() {
        return field_3;
    }

    public void setField_3(int field3) {
        this.field_3 = field3;
    }

    public SubObjectNested() {};

    public SubObjectNested(
        int field3
    ) {
        this.field_3 = field3;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubObjectNested other = (SubObjectNested) o;
        return
            field_3 == other.field_3;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field_3
        );
    }
}

class SubArray {
    private int field_3;
    private SubArrayNested[] subArrayNesteds;
    private SubObjectNested subObjectNested;

    public int getField_3() {
        return field_3;
    }

    public void setField_3(int field3) {
        this.field_3 = field3;
    }

    public SubArrayNested[] getSubArrayNesteds() {
        return subArrayNesteds;
    }

    public void setSubArrayNesteds(SubArrayNested[] subArrayNesteds) {
        this.subArrayNesteds = subArrayNesteds;
    }

    public SubObjectNested getSubObjectNested() {
        return subObjectNested;
    }

    public void setSubObjectNested(SubObjectNested subObjectNested) {
        this.subObjectNested = subObjectNested;
    }

    public SubArray() {};

    public SubArray(
        int field3,
        SubArrayNested[] subArrayNesteds,
        SubObjectNested subObjectNested
    ) {
        this.field_3 = field3;
        this.subArrayNesteds = subArrayNesteds;
        this.subObjectNested = subObjectNested;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubArray other = (SubArray) o;
        return
            field_3 == other.field_3 &&
            Arrays.equals(subArrayNesteds, other.subArrayNesteds) &&
            Objects.equals(subObjectNested, other.subObjectNested);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field_3,
            subArrayNesteds,
            subObjectNested
        );
    }
}

// Subtype sub type description
class Subtype {
    private int id2;

    public int getId2() {
        return id2;
    }

    public void setId2(int id2) {
        this.id2 = id2;
    }

    public Subtype() {};

    public Subtype(
        int id2
    ) {
        this.id2 = id2;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Subtype other = (Subtype) o;
        return
            id2 == other.id2;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id2
        );
    }
}

@com.tigrisdata.db.annotation.TigrisCollection(value = "products")
public class Product implements TigrisCollectionType {
    private SubArray[] subArrays;
    @TigrisField(description = "sub type description")
    private Subtype subtype;

    public SubArray[] getSubArrays() {
        return subArrays;
    }

    public void setSubArrays(SubArray[] subArrays) {
        this.subArrays = subArrays;
    }

    public Subtype getSubtype() {
        return subtype;
    }

    public void setSubtype(Subtype subtype) {
        this.subtype = subtype;
    }

    public Product() {};

    public Product(
        SubArray[] subArrays,
        Subtype subtype
    ) {
        this.subArrays = subArrays;
        this.subtype = subtype;
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Product other = (Product) o;
        return
            Arrays.equals(subArrays, other.subArrays) &&
            Objects.equals(subtype, other.subtype);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            subArrays,
            subtype
        );
    }
}
`},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			w := bufio.NewWriter(&buf)
			var hasTime, hasUUID bool
			err := genCollectionSchema(w, []byte(v.in), &JSONToJava{}, &hasTime, &hasUUID)
			require.NoError(t, err)
			_ = w.Flush()
			assert.Equal(t, v.exp, buf.String())
		})
	}
}
