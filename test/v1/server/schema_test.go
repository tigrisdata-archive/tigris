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

//go:build integration

package server

import (
	api "github.com/tigrisdata/tigris/api/server/v1"
	"net/http"
	"testing"

	"github.com/tigrisdata/tigris/server/config"
)

func TestSchemaMigration(t *testing.T) {
	if config.DefaultConfig.Search.WriteEnabled {
		t.Skip("TypeSense doesn't support incompatible schema changes")
	}

	if !config.DefaultConfig.Schema.AllowIncompatible {
		t.Skip("incompatible schema changes disabled")
	}

	coll := t.Name()

	var testSchemaV1 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "integer",
				},
				"field2": Map{
					"type": "string",
				},
				"field3": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	// change field1 type to string and delete field3
	var testSchemaV2 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "string",
				},
				"field2": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	// change field2 type to integer and re-add field3
	var testSchemaV3 = Map{
		"schema": Map{
			"title":       coll,
			"description": "this schema is for " + coll + " tests",
			"properties": Map{
				"pkey_int": Map{
					"type": "integer",
				},
				"field1": Map{
					"type": "string",
				},
				"field2": Map{
					"type": "integer",
				},
				"field3": Map{
					"type": "string",
				},
			},
			"primary_key": []interface{}{"pkey_int"},
		},
	}

	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	createCollection(t, db, coll, testSchemaV1).Status(http.StatusOK)

	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 1, "field1": 123, "field2": "567", "field3": "fld3_v1"}}, true).Status(http.StatusOK)

	createCollection(t, db, coll, testSchemaV2).Status(http.StatusOK)

	// fails because field1 is from previous schema version
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 2, "field1": 1230, "field2": "5670"}}, true).Status(http.StatusBadRequest)
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 2, "field1": "1230", "field2": "5670"}}, true).Status(http.StatusOK)

	createCollection(t, db, coll, testSchemaV3).Status(http.StatusOK)

	// fails because field2 is from previous schema version
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 3, "field1": "12300", "field2": "sss56700"}}, true).Status(http.StatusBadRequest)
	insertDocuments(t, db, coll, []Doc{
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "fld3_v3"}}, true).Status(http.StatusOK)

	readAndValidate(t, db, coll, Map{}, nil, []Doc{
		{"pkey_int": 1, "field1": "123", "field2": 567}, // field3 should not reappear so as it was deleted in schema V2, while the row was updated before
		{"pkey_int": 2, "field1": "1230", "field2": 5670},
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "fld3_v3"}, // field3 appear because the row was updated after it was re-added in schema V3
	})

	// test update flow
	updateByFilter(t, db, coll,
		Map{
			"filter": Map{
				"$or": []Doc{
					{"pkey_int": 1},
					{"pkey_int": 2},
					{"pkey_int": 3},
				},
			},
		},
		Map{
			"fields": Map{
				"$set": Map{
					"field3": "new_val",
				},
			},
		},
		nil,
	)

	readAndValidate(t, db, coll, Map{}, nil, []Doc{
		{"pkey_int": 1, "field1": "123", "field2": 567, "field3": "new_val"}, // field3 should not reappear so as it was deleted in schema V2, while the row was updated before
		{"pkey_int": 2, "field1": "1230", "field2": 5670, "field3": "new_val"},
		{"pkey_int": 3, "field1": "12300", "field2": 56700, "field3": "new_val"}, // field3 appear because the row was updated after it was re-added in schema V3
	})
}

func TestDescribeCollectionSchemaFormat(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	createCollection(t, db, coll, testCreateSchema).Status(http.StatusOK)

	resp := describeCollection(t, db, coll, Map{"schema_format": "go,ts,java"})

	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collection", coll).
		ValueEqual("schema", map[string]string{
			"go":   "\ntype ArrayValue struct {\n\tId int64 `json:\"id\"`\n\tProduct string `json:\"product\"`\n}\n\n// ObjectValue object field\ntype ObjectValue struct {\n\tBignumber int64 `json:\"bignumber\"`\n\tName string `json:\"name\"`\n}\n\n// TestCollection this schema is for integration tests\ntype TestCollection struct {\n\t// AddedStringValue simple string field\n\tAddedStringValue string `json:\"added_string_value\"`\n\t// AddedValueDouble simple double field\n\tAddedValueDouble float64 `json:\"added_value_double\"`\n\t// ArrayValues array field\n\tArrayValues []ArrayValue `json:\"array_value\"`\n\t// BoolValue simple boolean field\n\tBoolValue bool `json:\"bool_value\"`\n\t// BytesValue simple bytes field\n\tBytesValue []byte `json:\"bytes_value\"`\n\t// DateTimeValue date time field\n\tDateTimeValue time.Time `json:\"date_time_value\"`\n\t// DoubleValue simple double field\n\tDoubleValue float64 `json:\"double_value\"`\n\t// IntValue simple int field\n\tIntValue int64 `json:\"int_value\"`\n\t// ObjectValue object field\n\tObjectValue ObjectValue `json:\"object_value\"`\n\t// PkeyInt primary key field\n\tPkeyInt int64 `json:\"pkey_int\" tigris:\"primaryKey:1\"`\n\t// SimpleArrayValues array field\n\tSimpleArrayValues []string `json:\"simple_array_value\"`\n\t// StringValue simple string field\n\tStringValue string `json:\"string_value\" tigris:\"maxLength:128\"`\n\t// UuidValue uuid field\n\tUuidValue uuid.UUID `json:\"uuid_value\"`\n}\n",
			"java": "\nclass ArrayValue {\n    private long id;\n    private String product;\n\n    public long getId() {\n        return id;\n    }\n\n    public void setId(long id) {\n        this.id = id;\n    }\n\n    public String getProduct() {\n        return product;\n    }\n\n    public void setProduct(String product) {\n        this.product = product;\n    }\n\n    public ArrayValue() {};\n\n    public ArrayValue(\n        long id,\n        String product\n    ) {\n        this.id = id;\n        this.product = product;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ArrayValue other = (ArrayValue) o;\n        return\n            id == other.id \u0026\u0026\n            product == other.product;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            id,\n            product\n        );\n    }\n}\n\n// ObjectValue object field\nclass ObjectValue {\n    private long bignumber;\n    private String name;\n\n    public long getBignumber() {\n        return bignumber;\n    }\n\n    public void setBignumber(long bignumber) {\n        this.bignumber = bignumber;\n    }\n\n    public String getName() {\n        return name;\n    }\n\n    public void setName(String name) {\n        this.name = name;\n    }\n\n    public ObjectValue() {};\n\n    public ObjectValue(\n        long bignumber,\n        String name\n    ) {\n        this.bignumber = bignumber;\n        this.name = name;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ObjectValue other = (ObjectValue) o;\n        return\n            bignumber == other.bignumber \u0026\u0026\n            name == other.name;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            bignumber,\n            name\n        );\n    }\n}\n\n// TestCollection this schema is for integration tests\n@com.tigrisdata.db.annotation.TigrisCollection(value = \"test_collection\")\npublic class TestCollection implements TigrisCollectionType {\n    @TigrisField(description = \"simple string field\")\n    private String added_string_value;\n    @TigrisField(description = \"simple double field\")\n    private double added_value_double;\n    @TigrisField(description = \"array field\")\n    private ArrayValue[] array_value;\n    @TigrisField(description = \"simple boolean field\")\n    private boolean bool_value;\n    @TigrisField(description = \"simple bytes field\")\n    private byte[] bytes_value;\n    @TigrisField(description = \"date time field\")\n    private Date date_time_value;\n    @TigrisField(description = \"simple double field\")\n    private double double_value;\n    @TigrisField(description = \"simple int field\")\n    private long int_value;\n    @TigrisField(description = \"object field\")\n    private ObjectValue object_value;\n    @TigrisField(description = \"primary key field\")\n    @TigrisPrimaryKey(order = 1)\n    private long pkey_int;\n    @TigrisField(description = \"array field\")\n    private String[] simple_array_value;\n    @TigrisField(description = \"simple string field\")\n    private String string_value;\n    @TigrisField(description = \"uuid field\")\n    private UUID uuid_value;\n\n    public String getAdded_string_value() {\n        return added_string_value;\n    }\n\n    public void setAdded_string_value(String addedStringValue) {\n        this.added_string_value = addedStringValue;\n    }\n\n    public double getAdded_value_double() {\n        return added_value_double;\n    }\n\n    public void setAdded_value_double(double addedValueDouble) {\n        this.added_value_double = addedValueDouble;\n    }\n\n    public ArrayValue[] getArray_value() {\n        return array_value;\n    }\n\n    public void setArray_value(ArrayValue[] arrayValues) {\n        this.array_value = arrayValues;\n    }\n\n    public boolean isBool_value() {\n        return bool_value;\n    }\n\n    public void setBool_value(boolean boolValue) {\n        this.bool_value = boolValue;\n    }\n\n    public byte[] getBytes_value() {\n        return bytes_value;\n    }\n\n    public void setBytes_value(byte[] bytesValue) {\n        this.bytes_value = bytesValue;\n    }\n\n    public Date getDate_time_value() {\n        return date_time_value;\n    }\n\n    public void setDate_time_value(Date dateTimeValue) {\n        this.date_time_value = dateTimeValue;\n    }\n\n    public double getDouble_value() {\n        return double_value;\n    }\n\n    public void setDouble_value(double doubleValue) {\n        this.double_value = doubleValue;\n    }\n\n    public long getInt_value() {\n        return int_value;\n    }\n\n    public void setInt_value(long intValue) {\n        this.int_value = intValue;\n    }\n\n    public ObjectValue getObject_value() {\n        return object_value;\n    }\n\n    public void setObject_value(ObjectValue objectValue) {\n        this.object_value = objectValue;\n    }\n\n    public long getPkey_int() {\n        return pkey_int;\n    }\n\n    public void setPkey_int(long pkeyInt) {\n        this.pkey_int = pkeyInt;\n    }\n\n    public String[] getSimple_array_value() {\n        return simple_array_value;\n    }\n\n    public void setSimple_array_value(String[] simpleArrayValues) {\n        this.simple_array_value = simpleArrayValues;\n    }\n\n    public String getString_value() {\n        return string_value;\n    }\n\n    public void setString_value(String stringValue) {\n        this.string_value = stringValue;\n    }\n\n    public UUID getUuid_value() {\n        return uuid_value;\n    }\n\n    public void setUuid_value(UUID uuidValue) {\n        this.uuid_value = uuidValue;\n    }\n\n    public TestCollection() {};\n\n    public TestCollection(\n        String addedStringValue,\n        double addedValueDouble,\n        ArrayValue[] arrayValues,\n        boolean boolValue,\n        byte[] bytesValue,\n        Date dateTimeValue,\n        double doubleValue,\n        long intValue,\n        ObjectValue objectValue,\n        long pkeyInt,\n        String[] simpleArrayValues,\n        String stringValue,\n        UUID uuidValue\n    ) {\n        this.added_string_value = addedStringValue;\n        this.added_value_double = addedValueDouble;\n        this.array_value = arrayValues;\n        this.bool_value = boolValue;\n        this.bytes_value = bytesValue;\n        this.date_time_value = dateTimeValue;\n        this.double_value = doubleValue;\n        this.int_value = intValue;\n        this.object_value = objectValue;\n        this.pkey_int = pkeyInt;\n        this.simple_array_value = simpleArrayValues;\n        this.string_value = stringValue;\n        this.uuid_value = uuidValue;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        TestCollection other = (TestCollection) o;\n        return\n            added_string_value == other.added_string_value \u0026\u0026\n            added_value_double == other.added_value_double \u0026\u0026\n            Arrays.equals(array_value, other.array_value) \u0026\u0026\n            bool_value == other.bool_value \u0026\u0026\n            bytes_value == other.bytes_value \u0026\u0026\n            date_time_value == other.date_time_value \u0026\u0026\n            double_value == other.double_value \u0026\u0026\n            int_value == other.int_value \u0026\u0026\n            Objects.equals(object_value, other.object_value) \u0026\u0026\n            pkey_int == other.pkey_int \u0026\u0026\n            Arrays.equals(simple_array_value, other.simple_array_value) \u0026\u0026\n            string_value == other.string_value \u0026\u0026\n            uuid_value == other.uuid_value;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            added_string_value,\n            added_value_double,\n            array_value,\n            bool_value,\n            bytes_value,\n            date_time_value,\n            double_value,\n            int_value,\n            object_value,\n            pkey_int,\n            simple_array_value,\n            string_value,\n            uuid_value\n        );\n    }\n}\n",
			"ts":   "\nexport class ArrayValue {\n  @Field(TigrisDataTypes.INT64)\n  id: string;\n\n  @Field()\n  product: string;\n};\n\n// object field\nexport class ObjectValue {\n  @Field(TigrisDataTypes.INT64)\n  bignumber: string;\n\n  @Field()\n  name: string;\n};\n\n// this schema is for integration tests\n@TigrisCollection(\"test_collection\")\nexport class TestCollection {\n  // simple string field\n  @Field()\n  added_string_value: string;\n\n  // simple double field\n  @Field()\n  added_value_double: number;\n\n  // array field\n  @Field({ elements: ArrayValue })\n  array_value: Array<ArrayValue>;\n\n  // simple boolean field\n  @Field()\n  bool_value: boolean;\n\n  // simple bytes field\n  @Field(TigrisDataTypes.BYTE_STRING)\n  bytes_value: string;\n\n  // date time field\n  @Field(TigrisDataTypes.DATE_TIME)\n  date_time_value: Date;\n\n  // simple double field\n  @Field()\n  double_value: number;\n\n  // simple int field\n  @Field(TigrisDataTypes.INT64)\n  int_value: string;\n\n  // object field\n  @Field()\n  object_value: ObjectValue;\n\n  // primary key field\n  @PrimaryKey(TigrisDataTypes.INT64, { order: 1 })\n  pkey_int: string;\n\n  // array field\n  @Field({ elements: TigrisDataTypes.STRING })\n  simple_array_value: Array<string>;\n\n  // simple string field\n  @Field({ maxLength: 128 })\n  string_value: string;\n\n  // uuid field\n  @Field(TigrisDataTypes.UUID)\n  uuid_value: string;\n};\n",
		})

	resp = describeCollection(t, db, coll, Map{"schema_format": "go"})

	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collection", coll).
		ValueEqual("schema", map[string]string{
			"go": "\ntype ArrayValue struct {\n\tId int64 `json:\"id\"`\n\tProduct string `json:\"product\"`\n}\n\n// ObjectValue object field\ntype ObjectValue struct {\n\tBignumber int64 `json:\"bignumber\"`\n\tName string `json:\"name\"`\n}\n\n// TestCollection this schema is for integration tests\ntype TestCollection struct {\n\t// AddedStringValue simple string field\n\tAddedStringValue string `json:\"added_string_value\"`\n\t// AddedValueDouble simple double field\n\tAddedValueDouble float64 `json:\"added_value_double\"`\n\t// ArrayValues array field\n\tArrayValues []ArrayValue `json:\"array_value\"`\n\t// BoolValue simple boolean field\n\tBoolValue bool `json:\"bool_value\"`\n\t// BytesValue simple bytes field\n\tBytesValue []byte `json:\"bytes_value\"`\n\t// DateTimeValue date time field\n\tDateTimeValue time.Time `json:\"date_time_value\"`\n\t// DoubleValue simple double field\n\tDoubleValue float64 `json:\"double_value\"`\n\t// IntValue simple int field\n\tIntValue int64 `json:\"int_value\"`\n\t// ObjectValue object field\n\tObjectValue ObjectValue `json:\"object_value\"`\n\t// PkeyInt primary key field\n\tPkeyInt int64 `json:\"pkey_int\" tigris:\"primaryKey:1\"`\n\t// SimpleArrayValues array field\n\tSimpleArrayValues []string `json:\"simple_array_value\"`\n\t// StringValue simple string field\n\tStringValue string `json:\"string_value\" tigris:\"maxLength:128\"`\n\t// UuidValue uuid field\n\tUuidValue uuid.UUID `json:\"uuid_value\"`\n}\n",
		})

	// cleanup
	dropCollection(t, db, coll)
}

func TestDescribeDatabaseSchemaFormat(t *testing.T) {
	db, _ := setupTests(t)
	defer cleanupTests(t, db)

	resp := describeDatabase(t, db, Map{"schema_format": "   TypeScript,   Golang,java"})
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collections", []Map{
			{
				"size":       0,
				"metadata":   Map{},
				"collection": "test_collection",
				"schema": Map{
					"TypeScript": "\nexport class ArrayValue {\n  @Field(TigrisDataTypes.INT64)\n  id: string;\n\n  @Field()\n  product: string;\n};\n\n// object field\nexport class ObjectValue {\n  @Field(TigrisDataTypes.INT64)\n  bignumber: string;\n\n  @Field()\n  name: string;\n};\n\n// this schema is for integration tests\n@TigrisCollection(\"test_collection\")\nexport class TestCollection {\n  // simple string field\n  @Field()\n  added_string_value: string;\n\n  // simple double field\n  @Field()\n  added_value_double: number;\n\n  // array field\n  @Field({ elements: ArrayValue })\n  array_value: Array<ArrayValue>;\n\n  // simple boolean field\n  @Field()\n  bool_value: boolean;\n\n  // simple bytes field\n  @Field(TigrisDataTypes.BYTE_STRING)\n  bytes_value: string;\n\n  // date time field\n  @Field(TigrisDataTypes.DATE_TIME)\n  date_time_value: Date;\n\n  // simple double field\n  @Field()\n  double_value: number;\n\n  // simple int field\n  @Field(TigrisDataTypes.INT64)\n  int_value: string;\n\n  // object field\n  @Field()\n  object_value: ObjectValue;\n\n  // primary key field\n  @PrimaryKey(TigrisDataTypes.INT64, { order: 1 })\n  pkey_int: string;\n\n  // array field\n  @Field({ elements: TigrisDataTypes.STRING })\n  simple_array_value: Array<string>;\n\n  // simple string field\n  @Field({ maxLength: 128 })\n  string_value: string;\n\n  // uuid field\n  @Field(TigrisDataTypes.UUID)\n  uuid_value: string;\n};\n",
					"Golang":     "\ntype ArrayValue struct {\n\tId int64 `json:\"id\"`\n\tProduct string `json:\"product\"`\n}\n\n// ObjectValue object field\ntype ObjectValue struct {\n\tBignumber int64 `json:\"bignumber\"`\n\tName string `json:\"name\"`\n}\n\n// TestCollection this schema is for integration tests\ntype TestCollection struct {\n\t// AddedStringValue simple string field\n\tAddedStringValue string `json:\"added_string_value\"`\n\t// AddedValueDouble simple double field\n\tAddedValueDouble float64 `json:\"added_value_double\"`\n\t// ArrayValues array field\n\tArrayValues []ArrayValue `json:\"array_value\"`\n\t// BoolValue simple boolean field\n\tBoolValue bool `json:\"bool_value\"`\n\t// BytesValue simple bytes field\n\tBytesValue []byte `json:\"bytes_value\"`\n\t// DateTimeValue date time field\n\tDateTimeValue time.Time `json:\"date_time_value\"`\n\t// DoubleValue simple double field\n\tDoubleValue float64 `json:\"double_value\"`\n\t// IntValue simple int field\n\tIntValue int64 `json:\"int_value\"`\n\t// ObjectValue object field\n\tObjectValue ObjectValue `json:\"object_value\"`\n\t// PkeyInt primary key field\n\tPkeyInt int64 `json:\"pkey_int\" tigris:\"primaryKey:1\"`\n\t// SimpleArrayValues array field\n\tSimpleArrayValues []string `json:\"simple_array_value\"`\n\t// StringValue simple string field\n\tStringValue string `json:\"string_value\" tigris:\"maxLength:128\"`\n\t// UuidValue uuid field\n\tUuidValue uuid.UUID `json:\"uuid_value\"`\n}\n",
					"java":       "\nclass ArrayValue {\n    private long id;\n    private String product;\n\n    public long getId() {\n        return id;\n    }\n\n    public void setId(long id) {\n        this.id = id;\n    }\n\n    public String getProduct() {\n        return product;\n    }\n\n    public void setProduct(String product) {\n        this.product = product;\n    }\n\n    public ArrayValue() {};\n\n    public ArrayValue(\n        long id,\n        String product\n    ) {\n        this.id = id;\n        this.product = product;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ArrayValue other = (ArrayValue) o;\n        return\n            id == other.id \u0026\u0026\n            product == other.product;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            id,\n            product\n        );\n    }\n}\n\n// ObjectValue object field\nclass ObjectValue {\n    private long bignumber;\n    private String name;\n\n    public long getBignumber() {\n        return bignumber;\n    }\n\n    public void setBignumber(long bignumber) {\n        this.bignumber = bignumber;\n    }\n\n    public String getName() {\n        return name;\n    }\n\n    public void setName(String name) {\n        this.name = name;\n    }\n\n    public ObjectValue() {};\n\n    public ObjectValue(\n        long bignumber,\n        String name\n    ) {\n        this.bignumber = bignumber;\n        this.name = name;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ObjectValue other = (ObjectValue) o;\n        return\n            bignumber == other.bignumber \u0026\u0026\n            name == other.name;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            bignumber,\n            name\n        );\n    }\n}\n\n// TestCollection this schema is for integration tests\n@com.tigrisdata.db.annotation.TigrisCollection(value = \"test_collection\")\npublic class TestCollection implements TigrisCollectionType {\n    @TigrisField(description = \"simple string field\")\n    private String added_string_value;\n    @TigrisField(description = \"simple double field\")\n    private double added_value_double;\n    @TigrisField(description = \"array field\")\n    private ArrayValue[] array_value;\n    @TigrisField(description = \"simple boolean field\")\n    private boolean bool_value;\n    @TigrisField(description = \"simple bytes field\")\n    private byte[] bytes_value;\n    @TigrisField(description = \"date time field\")\n    private Date date_time_value;\n    @TigrisField(description = \"simple double field\")\n    private double double_value;\n    @TigrisField(description = \"simple int field\")\n    private long int_value;\n    @TigrisField(description = \"object field\")\n    private ObjectValue object_value;\n    @TigrisField(description = \"primary key field\")\n    @TigrisPrimaryKey(order = 1)\n    private long pkey_int;\n    @TigrisField(description = \"array field\")\n    private String[] simple_array_value;\n    @TigrisField(description = \"simple string field\")\n    private String string_value;\n    @TigrisField(description = \"uuid field\")\n    private UUID uuid_value;\n\n    public String getAdded_string_value() {\n        return added_string_value;\n    }\n\n    public void setAdded_string_value(String addedStringValue) {\n        this.added_string_value = addedStringValue;\n    }\n\n    public double getAdded_value_double() {\n        return added_value_double;\n    }\n\n    public void setAdded_value_double(double addedValueDouble) {\n        this.added_value_double = addedValueDouble;\n    }\n\n    public ArrayValue[] getArray_value() {\n        return array_value;\n    }\n\n    public void setArray_value(ArrayValue[] arrayValues) {\n        this.array_value = arrayValues;\n    }\n\n    public boolean isBool_value() {\n        return bool_value;\n    }\n\n    public void setBool_value(boolean boolValue) {\n        this.bool_value = boolValue;\n    }\n\n    public byte[] getBytes_value() {\n        return bytes_value;\n    }\n\n    public void setBytes_value(byte[] bytesValue) {\n        this.bytes_value = bytesValue;\n    }\n\n    public Date getDate_time_value() {\n        return date_time_value;\n    }\n\n    public void setDate_time_value(Date dateTimeValue) {\n        this.date_time_value = dateTimeValue;\n    }\n\n    public double getDouble_value() {\n        return double_value;\n    }\n\n    public void setDouble_value(double doubleValue) {\n        this.double_value = doubleValue;\n    }\n\n    public long getInt_value() {\n        return int_value;\n    }\n\n    public void setInt_value(long intValue) {\n        this.int_value = intValue;\n    }\n\n    public ObjectValue getObject_value() {\n        return object_value;\n    }\n\n    public void setObject_value(ObjectValue objectValue) {\n        this.object_value = objectValue;\n    }\n\n    public long getPkey_int() {\n        return pkey_int;\n    }\n\n    public void setPkey_int(long pkeyInt) {\n        this.pkey_int = pkeyInt;\n    }\n\n    public String[] getSimple_array_value() {\n        return simple_array_value;\n    }\n\n    public void setSimple_array_value(String[] simpleArrayValues) {\n        this.simple_array_value = simpleArrayValues;\n    }\n\n    public String getString_value() {\n        return string_value;\n    }\n\n    public void setString_value(String stringValue) {\n        this.string_value = stringValue;\n    }\n\n    public UUID getUuid_value() {\n        return uuid_value;\n    }\n\n    public void setUuid_value(UUID uuidValue) {\n        this.uuid_value = uuidValue;\n    }\n\n    public TestCollection() {};\n\n    public TestCollection(\n        String addedStringValue,\n        double addedValueDouble,\n        ArrayValue[] arrayValues,\n        boolean boolValue,\n        byte[] bytesValue,\n        Date dateTimeValue,\n        double doubleValue,\n        long intValue,\n        ObjectValue objectValue,\n        long pkeyInt,\n        String[] simpleArrayValues,\n        String stringValue,\n        UUID uuidValue\n    ) {\n        this.added_string_value = addedStringValue;\n        this.added_value_double = addedValueDouble;\n        this.array_value = arrayValues;\n        this.bool_value = boolValue;\n        this.bytes_value = bytesValue;\n        this.date_time_value = dateTimeValue;\n        this.double_value = doubleValue;\n        this.int_value = intValue;\n        this.object_value = objectValue;\n        this.pkey_int = pkeyInt;\n        this.simple_array_value = simpleArrayValues;\n        this.string_value = stringValue;\n        this.uuid_value = uuidValue;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        TestCollection other = (TestCollection) o;\n        return\n            added_string_value == other.added_string_value \u0026\u0026\n            added_value_double == other.added_value_double \u0026\u0026\n            Arrays.equals(array_value, other.array_value) \u0026\u0026\n            bool_value == other.bool_value \u0026\u0026\n            bytes_value == other.bytes_value \u0026\u0026\n            date_time_value == other.date_time_value \u0026\u0026\n            double_value == other.double_value \u0026\u0026\n            int_value == other.int_value \u0026\u0026\n            Objects.equals(object_value, other.object_value) \u0026\u0026\n            pkey_int == other.pkey_int \u0026\u0026\n            Arrays.equals(simple_array_value, other.simple_array_value) \u0026\u0026\n            string_value == other.string_value \u0026\u0026\n            uuid_value == other.uuid_value;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            added_string_value,\n            added_value_double,\n            array_value,\n            bool_value,\n            bytes_value,\n            date_time_value,\n            double_value,\n            int_value,\n            object_value,\n            pkey_int,\n            simple_array_value,\n            string_value,\n            uuid_value\n        );\n    }\n}\n",
				},
			},
		})
}

func TestInsert_SchemaValidationRequired(t *testing.T) {
	testCreateSchema["schema"].(Map)["required"] = []string{"string_value"}
	testCreateSchema["schema"].(Map)["properties"].(Map)["object_value"].(Map)["required"] = []string{"name"}
	testCreateSchema["schema"].(Map)["properties"].(Map)["array_value"].(Map)["items"].(Map)["required"] = []string{"id"}
	defer func() {
		delete(testCreateSchema["schema"].(Map), "required")
		delete(testCreateSchema["schema"].(Map)["properties"].(Map)["object_value"].(Map), "required")
		delete(testCreateSchema["schema"].(Map)["properties"].(Map)["array_value"].(Map)["items"].(Map), "required")
	}()

	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		documents  []Doc
		expMessage string
	}{
		{
			[]Doc{
				{
					"pkey_int": 1,
					"object_value": Map{
						"name": "bbb",
					},
				},
			},
			"json schema validation failed for field '' reason 'missing properties: 'string_value''",
		},
		{
			[]Doc{
				{
					"pkey_int":     1,
					"string_value": nil,
					"object_value": Map{
						"name": "bbb",
					},
				},
			},
			"json schema validation failed for field 'string_value' reason 'expected string, but got null'",
		},
		{
			[]Doc{
				{
					"pkey_int":     1,
					"string_value": "aaa",
					"object_value": Map{
						"noname": "bbb",
					},
				},
			},
			"jsonschema: '/object_value' does not validate with file:///server/test_collection.json#/properties/object_value/required: missing properties: 'name'",
		},
		{
			[]Doc{
				{
					"pkey_int":     1,
					"string_value": "aaa",
					"object_value": Map{
						"name": "bbb",
					},
					"array_value": []Doc{
						{
							"product": "foo",
						},
					},
				},
			},

			"json schema validation failed for field 'array_value/0' reason 'missing properties: 'id''",
		},
	}
	for _, c := range cases {
		resp := insertDocuments(t, db, coll, c.documents, true)

		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, c.expMessage)
	}
}

func TestInsert_SchemaValidationError(t *testing.T) {
	db, coll := setupTests(t)
	defer cleanupTests(t, db)

	cases := []struct {
		documents  []Doc
		expMessage string
	}{
		{
			[]Doc{
				{
					"pkey_int":  1,
					"int_value": 10.20,
				},
			},
			"json schema validation failed for field 'int_value' reason 'expected integer or null, but got number'",
		}, {
			[]Doc{
				{
					"pkey_int":     1,
					"string_value": 12,
				},
			},
			"json schema validation failed for field 'string_value' reason 'expected string or null, but got number'",
		}, {
			[]Doc{{"bytes_value": 12.30}},
			"json schema validation failed for field 'bytes_value' reason 'expected string or null, but got number'",
		}, {
			[]Doc{{"bytes_value": "not enough"}},
			"json schema validation failed for field 'bytes_value' reason ''not enough' is not valid 'byte''",
		}, {
			[]Doc{{"date_time_value": "Mon, 02 Jan 2006"}},
			"json schema validation failed for field 'date_time_value' reason ''Mon, 02 Jan 2006' is not valid 'date-time''",
		}, {
			[]Doc{{"uuid_value": "abc-bcd"}},
			"json schema validation failed for field 'uuid_value' reason ''abc-bcd' is not valid 'uuid''",
		}, {
			[]Doc{
				{
					"pkey_int":  10,
					"extra_key": "abc-bcd",
				},
			},
			"json schema validation failed for field '' reason 'additionalProperties 'extra_key' not allowed'",
		},
	}
	for _, c := range cases {
		resp := insertDocuments(t, db, coll, c.documents, true)

		testError(resp, http.StatusBadRequest, api.Code_INVALID_ARGUMENT, c.expMessage)
	}
}
