#!/bin/bash

if [ -z "$cli" ]; then
	cli="./tigris"
fi

test_search_import_null() {
  $cli search import --project=db_search_import_test import_null --create-index '{ "str_field" : null}' '{"str_field": "str12"}'

  exp_out='{
  "index": "import_null",
  "schema": {
    "properties": {
      "str_field": {
        "type": "string"
      }
    },
    "source": {
      "type": "external"
    },
    "title": "import_null"
  }
}'

  out=$($cli search index describe --project=db_search_import_test import_null|jq -S .)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_search_import_all_types() {
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli search import --project=db_search_import_test import_test --create-index
{
	"str_field" : "str_value",
	"int_field" : 1,
	"float_field" : 1.1,
	"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
	"time_field" : "2022-11-04T16:17:23.967964263-07:00",
	"bool_field" : true,
	"objects" : {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true
	},
	"arrays" : [ {
		"str_field" : "str_value",
		"int_field" : 1,
		"float_field" : 1.1,
		"uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1",
		"time_field" : "2022-11-04T16:17:23.967964263-07:00",
		"bool_field" : true
	} ],
    "prim_array" : [ "str" ]
}
EOF

  exp_out='{
  "index": "import_test",
  "schema": {
    "title": "import_test",
    "properties": {
      "arrays": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
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
        "format": "uuid"
      }
    },
    "source": {
      "type": "external"
    }
  }
}'

  out=$($cli search index describe --project=db_search_import_test import_test)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_search_evolve_schema() {
  # evolve schema in a batch
  $cli search import --project=db_search_import_test import_test1 --create-index '{ "id" : "1", "str_field" : "str_value" }' '{ "id" : "2", "int_field": 1 }'

  exp_out='{
  "index": "import_test1",
  "schema": {
    "title": "import_test1",
    "properties": {
      "id": {
        "type": "string"
      },
      "int_field": {
        "type": "integer"
      },
      "str_field": {
        "type": "string"
      }
    },
    "source": {
      "type": "external"
    }
  }
}'

  out=$($cli search index describe --project=db_search_import_test import_test1)
  diff -w -u <(echo "$exp_out") <(echo "$out")

  # evolve schema
  $cli search import --project=db_search_import_test import_test1 --update-schema '{ "id" : "3", "uuid_field" : "1ed6ff32-4c0f-4553-9cd3-a2ea3d58e9d1" }'

  exp_out='{
  "index": "import_test1",
  "schema": {
    "properties": {
      "id": {
        "type": "string"
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
    "source": {
      "type": "external"
    },
    "title": "import_test1"
  }
}'

  out=$($cli search index describe --project=db_search_import_test import_test1|jq -S .)
  diff -w -u <(echo "$exp_out") <(echo "$out")
}

test_search_import() {
  $cli delete-project -f db_search_import_test || true
  $cli create project db_search_import_test

  test_search_import_null
  test_search_import_all_types

  error "search index not found 'import_test_search_no_create'"  $cli search import --project=db_search_import_test import_test_search_no_create '{ "str_field" : "str_value" }'

  test_search_evolve_schema

  $cli delete-project -f db_search_import_test
}
