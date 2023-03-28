#!/bin/bash

if [ -z "$cli" ]; then
	cli="./tigris"
fi

test_import_null() {
  $cli import --project=db_import_test import_null --create-collection '{"str_field" : null}' '{"str_field": "str12"}'

  exp_out='{
  "collection": "import_null",
  "schema": {
    "primary_key": [
      "id"
    ],
    "properties": {
      "id": {
        "autoGenerate": true,
        "format": "uuid",
        "type": "string"
      },
      "str_field": {
        "type": "string"
      }
    },
    "title": "import_null"
  }
}'

  out=$($cli describe collection --project=db_import_test import_null|jq -S .)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_import_all_types() {
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
}

test_evolve_schema() {
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
}

test_multi_pk() {
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
}

test_dynamic_batch_size() {
  data=$(head -c 1000 /dev/zero|base64)

  docs=""

  set +x

  # Create batch with 6 exponentially increasing in size documents.
  # Approx. sizes:
  # 1353
  # 2706
  # 5412
  # 10824
  # 21648
  # 43296

  # TODO: Use 7 when CDC is disabled in local container
  # 86592

  for i in $(seq 1 6); do
    # shellcheck disable=SC2089
    docs="$docs{\"string_field\":\"$data $i\"}\n"
    data="$data$data"
  done

  set -x

  # import should succeed by dynamically reducing the batch size
  # shellcheck disable=SC2086,SC2090
  echo -e $docs | $cli import --batch-size=100 --project=db_import_test import_test_dynamic_batch --create-collection
}

test_import() {
  $cli delete-project -f db_import_test || true
  $cli create project db_import_test

  test_dynamic_batch_size
  test_import_null
  test_import_all_types

  error "collection doesn't exist 'import_test_no_create'"  $cli import --project=db_import_test import_test_no_create '{ "str_field" : "str_value" }'

  test_evolve_schema
  test_multi_pk

  $cli delete-project -f db_import_test
}
