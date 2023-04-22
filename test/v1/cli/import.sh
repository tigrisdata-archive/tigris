#!/bin/bash

if [ -z "$cli" ]; then
	cli="./tigris"
fi

test_import_null() {
  $cli import --project=db_import_test import_null '{"str_field" : null}' '{"str_field": "str12"}'

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
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import --project=db_import_test import_test --primary-key=uuid_field --autogenerate=uuid_field
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
  $cli import --project=db_import_test import_test1 --primary-key=id '{ "id" : 1, "str_field" : "str_value" }' '{ "id" : 2, "int_field": 1 }'

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
  $cli import --project=db_import_test --append import_test1 '{ "id" : 3, "uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }'

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
  $cli import --project=db_import_test import_test_multi_pk --primary-key=str_field,str_field1 '{ "str_field" : "str_value", "str_field1" : "stf_value1" }'

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
  echo -e $docs | $cli import --batch-size=100 --project=db_import_test import_test_dynamic_batch
}

test_import() {
  $cli delete-project -f db_import_test || true
  $cli create project db_import_test

  test_csv_import_delimiter
  test_csv_import_all_types
  error "record on line 3: wrong number of fields" test_csv_import_not_equal_n_fields
  test_csv_import_leading_space

  test_dynamic_batch_size
  test_import_null
  test_import_all_types

  error "collection doesn't exist 'import_test_no_create'"  $cli import --no-create-collection --project=db_import_test import_test_no_create '{ "str_field" : "str_value" }'

  $cli import --project=db_import_test import_test_append '{ "str_field" : "str_value" }'
  error "collection exists. use --append if you need to add documents to existing collection" $cli import --project=db_import_test import_test_append '{ "str_field" : "str_value" }'

  test_evolve_schema
  test_multi_pk

  $cli delete-project -f db_import_test
}

test_csv_import_all_types() {
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import --project=db_import_test import_test_csv --primary-key=uuid_field --autogenerate=uuid_field
str_field,int_field,float_field,uuid_field,time_field,bool_field,binary_field,objects.str_field,objects.int_field,objects.float_field,objects.uuid_field,objects.time_field,objects.bool_field,objects.binary_field,arrays.str_field,arrays.int_field,arrays.float_field,arrays.uuid_field,arrays.time_field,arrays.bool_field,arrays.binary_field
str_value,1,1.1,1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1,2022-11-04T16:17:23.967964263-07:00,true,cGVlay1hLWJvbwo=,str_value,1,1.1,1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1,2022-11-04T16:17:23.967964263-07:00,true,cGVlay1hLWJvbwo=,str_value,1,1.1,1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1,2022-11-04T16:17:23.967964263-07:00,true,cGVlay1hLWJvbwo=
str_value,2,2.1,"2ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1","2012-11-04T16:17:23.967964263-07:00",false,"",,0,3.1,"1ed6ff32-4c0f-4554-9cd3-a2ea3d58e9d1","2002-11-04T16:17:23.967964263-07:00",false,"cGVlay1hLWJvbwo=","str_value",3,4.1,"5ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1","2000-11-04T16:17:23.967964263-07:00",true,"cGVlay1hLWJvbwo="
EOF

  exp_out='{
  "collection": "import_test_csv",
  "schema": {
    "title": "import_test_csv",
    "properties": {
      "arrays": {
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

  out=$($cli describe collection --project=db_import_test import_test_csv)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_csv_import_not_equal_n_fields() {
  cat <<EOF | $cli import --project=db_import_test import_test_csv_nf --primary-key=uuid_field
str_field,int_field,float_field,uuid_field
"str_value",1,1.1,"1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
str_value,2.1,"2ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
EOF
}

test_csv_import_leading_space() {
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import --project=db_import_test import_test_csv_ls --primary-key=uuid_field
str_field,float_field,uuid_field
 " str_value",  1.1, "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
  str_value, 2.1,"2ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
EOF

  exp_out='{
  "collection": "import_test_csv_ls",
  "schema": {
    "title": "import_test_csv_ls",
    "properties": {
      "float_field": {
        "type": "number"
      },
      "str_field": {
        "type": "string"
      },
      "uuid_field": {
        "type": "string",
        "format": "uuid"
      }
    },
    "primary_key": [
      "uuid_field"
    ]
  }
}'

  out=$($cli describe collection --project=db_import_test import_test_csv_ls)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_csv_import_delimiter() {
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import --project=db_import_test import_test_csv_delim --primary-key=uuid_field --csv-trim-leading-space --csv-delimiter=":" --csv-comment=";"
str_field:float_field:uuid_field
 " str_value":  1.1: "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"
; this is comment

  str_value: 2.1:"2ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"

EOF

  exp_out='{
  "collection": "import_test_csv_delim",
  "schema": {
    "title": "import_test_csv_delim",
    "properties": {
      "float_field": {
        "type": "number"
      },
      "str_field": {
        "type": "string"
      },
      "uuid_field": {
        "type": "string",
        "format": "uuid"
      }
    },
    "primary_key": [
      "uuid_field"
    ]
  }
}'

  out=$($cli describe collection --project=db_import_test import_test_csv_delim)
  diff -w -u <(echo "$exp_out") <(echo "$out")

  exp_out='{"float_field":1.1,"str_field":" str_value","uuid_field":"1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"}
  {"float_field":2.1,"str_field":"str_value","uuid_field":"2ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1"}'

  out=$($cli read --project=db_import_test import_test_csv_delim)
  diff -w -u <(echo "$exp_out") <(echo "$out")

}
