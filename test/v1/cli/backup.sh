#!/bin/bash

if [ -z "$cli" ]; then
	cli="./tigris"
fi

test_backup() {
  # Settings
  TESTDIR=/tmp/testdir
  TESTDB=backup_test
  TESTCOLL=backup_test
  SCHEMAFILE="${TESTDIR}/${TESTDB}.schema"
  DATAFILE="${TESTDIR}/${TESTDB}.${TESTCOLL}.backup"

  # Initialize test database
  rm -rf "${TESTDIR}"
  $cli delete-project -f "${TESTDB}" || true
  $cli create project "${TESTDB}"

  # Add test data
  cat <<EOF | TIGRIS_LOG_LEVEL=debug $cli import "--project=${TESTDB}" "${TESTCOLL}" --create-collection --primary-key=uuid_field --autogenerate=uuid_field
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

  # Run backup
  $cli backup -d "${TESTDIR}" "--projects=${TESTDB}"

  schema_out=$(cat $SCHEMAFILE)
  data_out=$(cat $DATAFILE)

  schema_expected='{"title":"backup_test","properties":{"arrays":{"type":"array","items":{"type":"object","properties":{"binary_field":{"type":"string","format":"byte"},"bool_field":{"type":"boolean"},"float_field":{"type":"number"},"int_field":{"type":"integer"},"str_field":{"type":"string"},"time_field":{"type":"string","format":"date-time"},"uuid_field":{"type":"string","format":"uuid"}}}},"binary_field":{"type":"string","format":"byte"},"bool_field":{"type":"boolean"},"float_field":{"type":"number"},"int_field":{"type":"integer"},"objects":{"type":"object","properties":{"binary_field":{"type":"string","format":"byte"},"bool_field":{"type":"boolean"},"float_field":{"type":"number"},"int_field":{"type":"integer"},"str_field":{"type":"string"},"time_field":{"type":"string","format":"date-time"},"uuid_field":{"type":"string","format":"uuid"}}},"prim_array":{"type":"array","items":{"type":"string"}},"str_field":{"type":"string"},"time_field":{"type":"string","format":"date-time"},"uuid_field":{"type":"string","format":"uuid","autoGenerate":true}},"primary_key":["uuid_field"]}'
  data_expected='{
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
  }'

  # Check results
  diff -w -u <(echo "${schema_out}") <(echo "${schema_expected}")
  diff -w -u <(echo "${data_out}") <(echo "${data_expected}")

  # Test restore
  RESTDB="${TESTDB}_restored"
  $cli delete-project -f "${RESTDB}" || true
  $cli restore -d "${TESTDIR}" --projects="${TESTDB}" --postfix=restored

  # Obtain schema and data from restored database
  schema_out=$($cli describe database --schema-only --project="${RESTDB}")
  data_out=$($cli read "--project=${RESTDB}" "${TESTCOLL}")

  diff -w -u <(echo "${schema_out}") <(echo "${schema_expected}")
  diff -w -u <(echo "${data_out}") <(echo "${data_expected}")

  rm -rf "${TESTDIR}"
}
