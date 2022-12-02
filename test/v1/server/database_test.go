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

//go:build integration

package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"gopkg.in/gavv/httpexpect.v1"
)

func getDatabaseURL(databaseName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/%s", databaseName, methodName)
}

func getProjectURL(projectName string, methodName string) string {
	return fmt.Sprintf("/v1/projects/%s/%s", projectName, methodName)
}

func beginTransactionURL(databaseName string) string {
	return fmt.Sprintf("/v1/projects/%s/database/transactions/begin", databaseName)
}

func TestCreateDatabase(t *testing.T) {
	deleteProject(t, "test_db")
	resp := createProject(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "project created successfully")
}

func TestCreateDatabaseInvalidName(t *testing.T) {
	invalidDbNames := []string{"", "$testdb", "testdb$", "test$db", "abstract", "yield"}
	for _, name := range invalidDbNames {
		resp := createProject(t, name)
		resp.Status(http.StatusBadRequest).
			JSON().
			Path("$.error").
			Object().
			ValueEqual("message", "invalid database name")
	}
}

func TestCreateDatabaseValidName(t *testing.T) {
	validDbNames := []string{"test-coll", "test_coll"}
	for _, name := range validDbNames {
		deleteProject(t, name)
		resp := createProject(t, name)
		resp.Status(http.StatusOK)
	}
}

func TestBeginTransaction(t *testing.T) {
	resp := beginTransaction(t, "test_db")
	cookieVal := resp.Cookie("Tigris-Tx-Id").Value()
	require.NotNil(t, t, cookieVal)
}

func TestDescribeDatabase(t *testing.T) {
	createCollection(t, "test_db", "test_collection", testCreateSchema).Status(http.StatusOK)
	resp := describeDatabase(t, "test_db", Map{})
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("size", 0)
}

func TestDescribeDatabaseSchemaFormat(t *testing.T) {
	createCollection(t, "test_db", "test_collection", testCreateSchema).Status(http.StatusOK)
	resp := describeDatabase(t, "test_db", Map{"schema_format": "   TypeScript,   Golang,java"})
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("collections", []Map{
			{
				"size":       0,
				"metadata":   Map{},
				"collection": "test_collection",
				"schema": Map{
					"TypeScript": "\ninterface ArrayValue {\n  id: string;\n  product: string;\n}\n\nconst arrayValueSchema: TigrisSchema\u003cArrayValue\u003e = {\n  id: {\n    type: TigrisDataTypes.INT64,\n  },\n  product: {\n    type: TigrisDataTypes.STRING,\n  },\n};\n\n// ObjectValue object field\ninterface ObjectValue {\n  bignumber: string;\n  name: string;\n}\n\nconst objectValueSchema: TigrisSchema\u003cObjectValue\u003e = {\n  bignumber: {\n    type: TigrisDataTypes.INT64,\n  },\n  name: {\n    type: TigrisDataTypes.STRING,\n  },\n};\n\n// TestCollection this schema is for integration tests\nexport interface TestCollection extends TigrisCollectionType {\n  // added_string_value simple string field\n  added_string_value: string;\n  // added_value_double simple double field\n  added_value_double: number;\n  // array_value array field\n  array_value: Array<ArrayValue>;\n  // bool_value simple boolean field\n  bool_value: boolean;\n  // bytes_value simple bytes field\n  bytes_value: string;\n  // date_time_value date time field\n  date_time_value: string;\n  // double_value simple double field\n  double_value: number;\n  // int_value simple int field\n  int_value: string;\n  // object_value object field\n  object_value: ObjectValue;\n  // pkey_int primary key field\n  pkey_int: string;\n  // string_value simple string field\n  string_value: string;\n  // uuid_value uuid field\n  uuid_value: string;\n}\n\nexport const testCollectionSchema: TigrisSchema\u003cTestCollection\u003e = {\n  added_string_value: {\n    type: TigrisDataTypes.STRING,\n  },\n  added_value_double: {\n    type: TigrisDataTypes.NUMBER,\n  },\n  array_value: {\n    type: TigrisDataTypes.ARRAY,\n    items: {\n      type: arrayValueSchema,\n    },\n  },\n  bool_value: {\n    type: TigrisDataTypes.BOOLEAN,\n  },\n  bytes_value: {\n    type: TigrisDataTypes.BYTE_STRING,\n  },\n  date_time_value: {\n    type: TigrisDataTypes.DATE_TIME,\n  },\n  double_value: {\n    type: TigrisDataTypes.NUMBER,\n  },\n  int_value: {\n    type: TigrisDataTypes.INT64,\n  },\n  object_value: {\n    type: objectValueSchema,\n  },\n  pkey_int: {\n    type: TigrisDataTypes.INT64,\n    primary_key: {\n      order: 1,\n    },\n  },\n  string_value: {\n    type: TigrisDataTypes.STRING,\n  },\n  uuid_value: {\n    type: TigrisDataTypes.UUID,\n  },\n};\n",
					"Golang":     "\ntype ArrayValue struct {\n\tId int64 `json:\"id\"`\n\tProduct string `json:\"product\"`\n}\n\n// ObjectValue object field\ntype ObjectValue struct {\n\tBignumber int64 `json:\"bignumber\"`\n\tName string `json:\"name\"`\n}\n\n// TestCollection this schema is for integration tests\ntype TestCollection struct {\n\t// AddedStringValue simple string field\n\tAddedStringValue string `json:\"added_string_value\"`\n\t// AddedValueDouble simple double field\n\tAddedValueDouble float64 `json:\"added_value_double\"`\n\t// ArrayValues array field\n\tArrayValues []ArrayValue `json:\"array_value\"`\n\t// BoolValue simple boolean field\n\tBoolValue bool `json:\"bool_value\"`\n\t// BytesValue simple bytes field\n\tBytesValue []byte `json:\"bytes_value\"`\n\t// DateTimeValue date time field\n\tDateTimeValue time.Time `json:\"date_time_value\"`\n\t// DoubleValue simple double field\n\tDoubleValue float64 `json:\"double_value\"`\n\t// IntValue simple int field\n\tIntValue int64 `json:\"int_value\"`\n\t// ObjectValue object field\n\tObjectValue ObjectValue `json:\"object_value\"`\n\t// PkeyInt primary key field\n\tPkeyInt int64 `json:\"pkey_int\" tigris:\"primaryKey:1\"`\n\t// StringValue simple string field\n\tStringValue string `json:\"string_value\"`\n\t// UuidValue uuid field\n\tUuidValue uuid.UUID `json:\"uuid_value\"`\n}\n",
					"java":       "\nclass ArrayValue {\n    private long id;\n    private String product;\n\n    public long getId() {\n        return id;\n    }\n\n    public void setId(long id) {\n        this.id = id;\n    }\n\n    public String getProduct() {\n        return product;\n    }\n\n    public void setProduct(String product) {\n        this.product = product;\n    }\n\n    public ArrayValue() {};\n\n    public ArrayValue(\n        long id,\n        String product\n    ) {\n        this.id = id;\n        this.product = product;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ArrayValue other = (ArrayValue) o;\n        return\n            id == other.id \u0026\u0026\n            product == other.product;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            id,\n            product\n        );\n    }\n}\n\n// ObjectValue object field\nclass ObjectValue {\n    private long bignumber;\n    private String name;\n\n    public long getBignumber() {\n        return bignumber;\n    }\n\n    public void setBignumber(long bignumber) {\n        this.bignumber = bignumber;\n    }\n\n    public String getName() {\n        return name;\n    }\n\n    public void setName(String name) {\n        this.name = name;\n    }\n\n    public ObjectValue() {};\n\n    public ObjectValue(\n        long bignumber,\n        String name\n    ) {\n        this.bignumber = bignumber;\n        this.name = name;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        ObjectValue other = (ObjectValue) o;\n        return\n            bignumber == other.bignumber \u0026\u0026\n            name == other.name;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            bignumber,\n            name\n        );\n    }\n}\n\n// TestCollection this schema is for integration tests\n@com.tigrisdata.db.annotation.TigrisCollection(value = \"test_collection\")\npublic class TestCollection implements TigrisDocumentCollectionType {\n    @TigrisField(description = \"simple string field\")\n    private String added_string_value;\n    @TigrisField(description = \"simple double field\")\n    private double added_value_double;\n    @TigrisField(description = \"array field\")\n    private ArrayValue[] array_value;\n    @TigrisField(description = \"simple boolean field\")\n    private boolean bool_value;\n    @TigrisField(description = \"simple bytes field\")\n    private byte[] bytes_value;\n    @TigrisField(description = \"date time field\")\n    private Date date_time_value;\n    @TigrisField(description = \"simple double field\")\n    private double double_value;\n    @TigrisField(description = \"simple int field\")\n    private long int_value;\n    @TigrisField(description = \"object field\")\n    private ObjectValue object_value;\n    @TigrisField(description = \"primary key field\")\n    @TigrisPrimaryKey(order = 1)\n    private long pkey_int;\n    @TigrisField(description = \"simple string field\")\n    private String string_value;\n    @TigrisField(description = \"uuid field\")\n    private UUID uuid_value;\n\n    public String getAdded_string_value() {\n        return added_string_value;\n    }\n\n    public void setAdded_string_value(String addedStringValue) {\n        this.added_string_value = addedStringValue;\n    }\n\n    public double getAdded_value_double() {\n        return added_value_double;\n    }\n\n    public void setAdded_value_double(double addedValueDouble) {\n        this.added_value_double = addedValueDouble;\n    }\n\n    public ArrayValue[] getArray_value() {\n        return array_value;\n    }\n\n    public void setArray_value(ArrayValue[] arrayValues) {\n        this.array_value = arrayValues;\n    }\n\n    public boolean isBool_value() {\n        return bool_value;\n    }\n\n    public void setBool_value(boolean boolValue) {\n        this.bool_value = boolValue;\n    }\n\n    public byte[] getBytes_value() {\n        return bytes_value;\n    }\n\n    public void setBytes_value(byte[] bytesValue) {\n        this.bytes_value = bytesValue;\n    }\n\n    public Date getDate_time_value() {\n        return date_time_value;\n    }\n\n    public void setDate_time_value(Date dateTimeValue) {\n        this.date_time_value = dateTimeValue;\n    }\n\n    public double getDouble_value() {\n        return double_value;\n    }\n\n    public void setDouble_value(double doubleValue) {\n        this.double_value = doubleValue;\n    }\n\n    public long getInt_value() {\n        return int_value;\n    }\n\n    public void setInt_value(long intValue) {\n        this.int_value = intValue;\n    }\n\n    public ObjectValue getObject_value() {\n        return object_value;\n    }\n\n    public void setObject_value(ObjectValue objectValue) {\n        this.object_value = objectValue;\n    }\n\n    public long getPkey_int() {\n        return pkey_int;\n    }\n\n    public void setPkey_int(long pkeyInt) {\n        this.pkey_int = pkeyInt;\n    }\n\n    public String getString_value() {\n        return string_value;\n    }\n\n    public void setString_value(String stringValue) {\n        this.string_value = stringValue;\n    }\n\n    public UUID getUuid_value() {\n        return uuid_value;\n    }\n\n    public void setUuid_value(UUID uuidValue) {\n        this.uuid_value = uuidValue;\n    }\n\n    public TestCollection() {};\n\n    public TestCollection(\n        String addedStringValue,\n        double addedValueDouble,\n        ArrayValue[] arrayValues,\n        boolean boolValue,\n        byte[] bytesValue,\n        Date dateTimeValue,\n        double doubleValue,\n        long intValue,\n        ObjectValue objectValue,\n        long pkeyInt,\n        String stringValue,\n        UUID uuidValue\n    ) {\n        this.added_string_value = addedStringValue;\n        this.added_value_double = addedValueDouble;\n        this.array_value = arrayValues;\n        this.bool_value = boolValue;\n        this.bytes_value = bytesValue;\n        this.date_time_value = dateTimeValue;\n        this.double_value = doubleValue;\n        this.int_value = intValue;\n        this.object_value = objectValue;\n        this.pkey_int = pkeyInt;\n        this.string_value = stringValue;\n        this.uuid_value = uuidValue;\n    };\n\n    @Override\n    public boolean equals(Object o) {\n        if (this == o) {\n            return true;\n        }\n        if (o == null || getClass() != o.getClass()) {\n            return false;\n        }\n\n        TestCollection other = (TestCollection) o;\n        return\n            added_string_value == other.added_string_value \u0026\u0026\n            added_value_double == other.added_value_double \u0026\u0026\n            Arrays.equals(array_value, other.array_value) \u0026\u0026\n            bool_value == other.bool_value \u0026\u0026\n            bytes_value == other.bytes_value \u0026\u0026\n            date_time_value == other.date_time_value \u0026\u0026\n            double_value == other.double_value \u0026\u0026\n            int_value == other.int_value \u0026\u0026\n            Objects.equals(object_value, other.object_value) \u0026\u0026\n            pkey_int == other.pkey_int \u0026\u0026\n            string_value == other.string_value \u0026\u0026\n            uuid_value == other.uuid_value;\n    }\n\n    @Override\n    public int hashCode() {\n        return Objects.hash(\n            added_string_value,\n            added_value_double,\n            array_value,\n            bool_value,\n            bytes_value,\n            date_time_value,\n            double_value,\n            int_value,\n            object_value,\n            pkey_int,\n            string_value,\n            uuid_value\n        );\n    }\n}\n",
				},
			},
		})
}

func TestDropDatabase_NotFound(t *testing.T) {
	resp := deleteProject(t, "test_drop_db_not_found")
	testError(resp, http.StatusNotFound, api.Code_NOT_FOUND, "database doesn't exist 'test_drop_db_not_found'")
}

func TestDropDatabase(t *testing.T) {
	resp := deleteProject(t, "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "project deleted successfully")
}

func createProject(t *testing.T, projectName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getProjectURL(projectName, "create")).
		Expect()
}

func beginTransaction(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(beginTransactionURL(databaseName)).
		Expect()
}

func deleteProject(t *testing.T, projectName string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getProjectURL(projectName, "delete")).
		Expect()
}

func describeDatabase(t *testing.T, databaseName string, req Map) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "describe")).WithJSON(req).Expect()
}
