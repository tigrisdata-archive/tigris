#!/bin/bash

if [ -z "$cli" ]; then
	cli="./tigris"
fi

test_import() {
  $cli delete-project -f db_import_test || true
  $cli create project db_import_test

  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import --project=db_import_test import_test --create-collection --primary-key=uuid_field --autogenerate=uuid_field
{
	"str_field" : "str_value",
	"int_field" : 1,
	"float_field" : 1.1,
	"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
	"time_field" : "2022-11-04T16:17:23.967964263-07:00",
	"bool_field" : true,
	"binary_field": "cGVlay1hLWJvbwo=",
	"objects" : {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true,
		"binary_field": "cGVlay1hLWJvbwo="
	},
	"arrays" : [ {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true,
		"binary_field": "cGVlay1hLWJvbwo="
	} ],
    "prim_array" : [ "str" ]
}
EOF

  exp_out='{
  "collection": "import_test",
  "schema": {
    "title": "import_test",
    "properties": {
      "arrays": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "binary_field": {
              "type": "string",
              "format": "byte"
            },
            "bool_field": {
              "type": "boolean"
            },
            "float_field": {
              "type": "number"
            },
            "int_field": {
              "type": "integer"
            },
            "str_field": {
              "type": "string"
            },
            "time_field": {
              "type": "string",
              "format": "date-time"
            },
            "uuid_field": {
              "type": "string",
              "format": "uuid"
            }
          }
        }
      },
      "binary_field": {
        "type": "string",
        "format": "byte"
      },
      "bool_field": {
        "type": "boolean"
      },
      "float_field": {
        "type": "number"
      },
      "int_field": {
        "type": "integer"
      },
      "objects": {
        "type": "object",
        "properties": {
          "binary_field": {
            "type": "string",
            "format": "byte"
          },
          "bool_field": {
            "type": "boolean"
          },
          "float_field": {
            "type": "number"
          },
          "int_field": {
            "type": "integer"
          },
          "str_field": {
            "type": "string"
          },
          "time_field": {
            "type": "string",
            "format": "date-time"
          },
          "uuid_field": {
            "type": "string",
            "format": "uuid"
          }
        }
      },
      "prim_array": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "str_field": {
        "type": "string"
      },
      "time_field": {
        "type": "string",
        "format": "date-time"
      },
      "uuid_field": {
        "type": "string",
        "format": "uuid",
        "autoGenerate": true
      }
    },
    "primary_key": [
      "uuid_field"
    ]
  }
}'

  out=$($cli describe collection --project=db_import_test import_test)
  diff -w -u <(echo "$exp_out") <(echo "$out")

  error "collection doesn't exist 'import_test_no_create'"  $cli import --project=db_import_test import_test_no_create '{ "str_field" : "str_value" }'

  # evolve schema in a batch
  $cli import --project=db_import_test import_test1 --create-collection --primary-key=id '{ "id" : 1, "str_field" : "str_value" }' '{ "id" : 2, "int_field": 1 }'

  exp_out='{
  "collection": "import_test1",
  "schema": {
    "title": "import_test1",
    "properties": {
      "id": {
        "type": "integer"
      },
      "int_field": {
        "type": "integer"
      },
      "str_field": {
        "type": "string"
      }
    },
    "primary_key": [
      "id"
    ]
  }
}'

  out=$($cli describe collection --project=db_import_test import_test1)
  diff -w -u <(echo "$exp_out") <(echo "$out")

  # evolve schema
  $cli import --project=db_import_test import_test1 '{ "id" : 3, "uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }'

  exp_out='{
  "collection": "import_test1",
  "schema": {
    "primary_key": [
      "id"
    ],
    "properties": {
      "id": {
        "type": "integer"
      },
      "int_field": {
        "type": "integer"
      },
      "str_field": {
        "type": "string"
      },
      "uuid_field": {
        "format": "uuid",
        "type": "string"
      }
    },
    "title": "import_test1"
  }
}'

  out=$($cli describe collection --project=db_import_test import_test1|jq -S .)
  diff -w -u <(echo "$exp_out") <(echo "$out")

  $cli import --project=db_import_test import_test_multi_pk --create-collection --primary-key=str_field,str_field1 '{ "str_field" : "str_value", "str_field1" : "stf_value1" }'

  exp_out='{
  "collection": "import_test_multi_pk",
  "schema": {
    "primary_key": [
      "str_field",
      "str_field1"
    ],
    "properties": {
      "str_field": {
        "type": "string"
      },
      "str_field1": {
        "type": "string"
      }
    },
    "title": "import_test_multi_pk"
  }
}'

  out=$($cli describe collection --project=db_import_test import_test_multi_pk|jq -S .)
  diff -w -u <(echo "$exp_out") <(echo "$out")

	$cli delete-project -f db_import_test
}
