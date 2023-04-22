#!/bin/bash
# Copyright 2022 Tigris Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex
PS4='${LINENO}: '

if [ -z "$cli" ]; then
	cli="$(pwd)/tigris"
fi

if [ -z "$TIGRIS_TEST_PORT" ]; then
	TIGRIS_TEST_PORT=8090
fi

unset TIGRIS_URL
unset TIGRIS_TOKEN
unset TIGRIS_CLIENT_SECRET
unset TIGRIS_CLIENT_ID

# Just to check if any config is set
env|grep TIGRIS || true

$cli version
$cli config show

def_cfg=$($cli config show)
if [ "$def_cfg" != '' ]; then
	set +x
	cat <<EOF

Unexpected default config

It can be caused by:
  * tigris-cli.yaml in /etc/tigris/, $HOME/tigris/, ./config/, . directories
    back it up and remove original one
  * TIGRIS_* environment variables set

EOF
	exit 1
fi

#shellcheck disable=SC2154
if [ -z "$noup" ]; then
	TIGRIS_LOG_LEVEL=debug $cli local up "$TIGRIS_TEST_PORT"
	$cli local logs >/dev/null 2>&1
fi


OS=$(uname -s)

export TIGRIS_URL="localhost:$TIGRIS_TEST_PORT"
$cli server info
$cli server version

if [ "$OS" == 'Darwin' ]; then
  export TIGRIS_TIMEOUT=25s
fi

test_config() {
  export TIGRIS_CLIENT_ID=test_id_1
  export TIGRIS_CLIENT_SECRET=test_secret_1
  export TIGRIS_TIMEOUT=333s
  export TIGRIS_PROTOCOL=https
  export TIGRIS_URL=example.com:8888
  export TIGRIS_PROJECT=test_proj1
  $cli config show | grep "client_id: test_id_1"
  $cli config show | grep "client_secret: test_secret_1"
  $cli config show | grep "timeout: 5m33s"
  $cli config show | grep "protocol: https"
  $cli config show | grep "url: example.com:8888"
  $cli config show | grep 'project: test_proj1'
  unset TIGRIS_PROTOCOL
  unset TIGRIS_URL
  unset TIGRIS_TIMEOUT
  unset TIGRIS_CLIENT_ID
  unset TIGRIS_CLIENT_SECRET
  unset TIGRIS_PROJECT
}

db_tests() {
	echo "=== Test ==="
	echo "Proto: $TIGRIS_PROTOCOL, URL: $TIGRIS_URL"
	echo "============"
	$cli ping

	$cli delete-project -f db1 || true

	$cli create project db1

	coll1='{"title":"coll1","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"},"Field2":{"type":"integer"}},"primary_key":["Key1"],"collection_type":"documents"}'
	coll111='{"title":"coll111","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"primary_key":["Key1"],"collection_type":"documents"}'

	#reading schemas from command line parameters
	$cli create collection "$coll1" "$coll111" --project=db1

	out=$($cli describe collection coll1 --project=db1 |tr -d '\n')
	diff -w -u <(echo '{"collection":"coll1","schema":'"$coll1"'}') <(echo "$out")

	out=$($cli describe database --project=db1 |tr -d '\n')
	# The output order is not-deterministic, try both combinations:
	# BUG: Http doesn't fill metadata due to omitempty json tag on protobuf generated structs

	if { [ "$TIGRIS_PROTOCOL" == "http" ]  || [[ "$TIGRIS_URL" == "http"* ]]; } && [[ "$TIGRIS_URL" != "grpc"* ]] ;
	then
			diff -w -u <(echo '{"db":"db1","collections":[{"collection":"coll1","schema":'"$coll1"'},{"collection":"coll111","schema":'"$coll111"'}]}') <(echo "$out") ||
			diff -w -u <(echo '{"db":"db1","collections":[{"collection":"coll111","schema":'"$coll111"'},{"collection":"coll1","schema":'"$coll1"'}]}') <(echo "$out") ||
			diff -w -u <(echo '{"collections":[{"collection":"coll111","schema":'"$coll111"'},{"collection":"coll1","schema":'"$coll1"'}]}') <(echo "$out") ||
			diff -w -u <(echo '{"collections":[{"collection":"coll1","schema":'"$coll1"'},{"collection":"coll111","schema":'"$coll111"'}]}') <(echo "$out")
	else
		diff -w -u <(echo '{"metadata":{},"db":"db1","collections":[{"collection":"coll1","schema":'"$coll1"'},{"collection":"coll111","schema":'"$coll111"'}]}') <(echo "$out") ||
		diff -w -u <(echo '{"metadata":{},"db":"db1","collections":[{"collection":"coll111","schema":'"$coll111"'},{"collection":"coll1","schema":'"$coll1"'}]}') <(echo "$out") ||
		diff -w -u <(echo '{"metadata":{},"collections":[{"collection":"coll111","schema":'"$coll111"'},{"collection":"coll1","schema":'"$coll1"'}]}') <(echo "$out") ||
		diff -w -u <(echo '{"metadata":{},"collections":[{"collection":"coll1","schema":'"$coll1"'},{"collection":"coll111","schema":'"$coll111"'}]}') <(echo "$out")
	fi

	out=$($cli describe database --project=db1 --schema-only|tr -d '\n')
	diff -w -u <(echo -e "$coll1$coll111") <(echo "$out") ||
	diff -w -u <(echo -e "$coll111$coll1") <(echo "$out")

	#reading schemas from stream
	# \n at the end to test empty line skipping
	# this also test multi-line streams
	echo -e '{ "title" : "coll2",
	"properties": { "Key1": { "type": "string" },
	"Field1": { "type": "integer" }, "Field2": { "type": "integer" } }, "primary_key": ["Key1"] }\n        \n\n' | $cli create collection --project=db1 -
	#reading array of schemas
	echo '[{ "title" : "coll3", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }, { "title" : "coll4", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }]' | $cli create collection --project=db1 -
	#reading schemas from command line array
	$cli create collection --project=db1 '[{ "title" : "coll5", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }, { "title" : "coll6", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }]' '{ "title" : "coll7", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }'
	# allow to skip - in non interactive input
	$cli create collection --project=db1 <<< '[{ "title" : "coll8", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }, { "title" : "coll9", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }]'

	$cli list projects
	$cli list collections --project=db1

	#insert from command line parameters
	$cli insert --project=db1 coll1 '{"Key1": "vK1", "Field1": 1}' \
		'{"Key1": "vK2", "Field1": 10}'

	#duplicate key
	$cli insert --project=db1 coll1 '{"Key1": "vK1", "Field1": 1}' && exit 1

	#insert from array
	$cli insert --project=db1 coll1 '[{"Key1": "vK7", "Field1": 1},
		{"Key1": "vK8", "Field1": 10}]'

	$cli replace --project=db1 coll1 '{"Key1": "vK1", "Field1": 1111}' \
		'{"Key1": "vK211", "Field1": 10111}'

	$cli replace --project=db1 coll1 '[{"Key1": "vK7", "Field1": 22222}]' \
		'[{"Key1": "vK2", "Field1": 10}]'

	#insert from standard input stream
	cat <<EOF | $cli insert "--project=db1" "coll1"
{"Key1": "vK10", "Field1": 10}
{"Key1": "vK20", "Field1": 20}
{"Key1": "vK30", "Field1": 30}
EOF

	cat <<EOF | $cli replace "--project=db1" "coll1"
{"Key1": "vK100", "Field1": 100}
{"Key1": "vK200", "Field1": 200}
{"Key1": "vK300", "Field1": 300}
EOF

	#insert from standard input array
	#NOTE: space and tabs are intentional. to test trim functionality
	cat <<EOF | $cli insert "--project=db1" "coll1" -
  	 [
{"Key1": "vK1011", "Field1": 1044},
{"Key1": "vK2011", "Field1": 2055},
{"Key1": "vK3011", "Field1": 3066}
]
EOF

	cat <<EOF | $cli replace --project=db1 coll1 -
[{"Key1": "vK101", "Field1": 104},
{"Key1": "vK202", "Field1": 205},
{"Key1": "vK303", "Field1": 306}]
EOF

	#copy collection content
	$cli read --project=db1 coll1 | $cli insert --project=db1 coll2 -

	exp_out='{"Key1": "vK1", "Field1": 1111}
{"Key1": "vK10", "Field1": 10}
{"Key1": "vK100", "Field1": 100}
{"Key1": "vK101", "Field1": 104}
{"Key1": "vK1011", "Field1": 1044}
{"Key1": "vK2", "Field1": 10}
{"Key1": "vK20", "Field1": 20}
{"Key1": "vK200", "Field1": 200}
{"Key1": "vK2011", "Field1": 2055}
{"Key1": "vK202", "Field1": 205}
{"Key1": "vK211", "Field1": 10111}
{"Key1": "vK30", "Field1": 30}
{"Key1": "vK300", "Field1": 300}
{"Key1": "vK3011", "Field1": 3066}
{"Key1": "vK303", "Field1": 306}
{"Key1": "vK7", "Field1": 22222}
{"Key1": "vK8", "Field1": 10}'

	out=$($cli read --project=db1 coll1 '{}')
	diff -w -u <(echo "$exp_out") <(echo "$out")

	out=$($cli read --project=db1 coll2 '{}')
	diff -w -u <(echo "$exp_out") <(echo "$out")

	# shellcheck disable=SC2016
	$cli update --project=db1 coll1 '{"Key1": "vK1"}' '{"$set" : {"Field1": 1000}}'

	out=$($cli read --project=db1 coll1 '{"Key1": "vK1"}' '{"Field1":true}')
	diff -w -u <(echo '{"Field1":1000}') <(echo "$out")

	$cli delete --project=db1 coll1 '{"Key1": "vK1"}'

	out=$($cli read "--project=db1" "coll1" '{"Key1": "vK1"}')
	[[ "$out" == '' ]] || exit 1

	$cli insert --project=db1 coll3 '{"Key1": "vK1", "Field1": 1}' \
		'{"Key1": "vK2", "Field1": 10}'

	cat <<'EOF' | $cli transact "--project=db1" -
[
{"insert" : { "collection" : "coll3", "documents": [{"Key1": "vK20000", "Field1": 20022}]}},
{"replace" : { "collection" : "coll3", "documents": [{"Key1": "vK30000", "Field1": 30033}]}},
{"update" : { "collection" : "coll3", "filter" : { "Key1": "vK2" }, "fields" : { "$set" : { "Field1" : 10000111 }}}},
{"delete" : { "collection" : "coll3", "filter" : { "Key1": "vK1" }}},
{"read"   : { "collection" : "coll3", "filter" : {}, "fields" : {}}}
]
EOF

	out=$($cli read --project=db1 coll3)
exp_out='{"Key1": "vK2", "Field1": 10000111}
{"Key1": "vK20000", "Field1": 20022}
{"Key1": "vK30000", "Field1": 30033}'
	diff -w -u <(echo "$exp_out") <(echo "$out")

	$cli insert --project=db1 coll4 '{"Key1": "vK1", "Field1": 1}' \
		'{"Key1": "vK2", "Field1": 10}'

	cat <<'EOF' | $cli transact "--project=db1" -
{"operation": "insert", "collection" : "coll4", "documents": [{"Key1": "vK200", "Field1": 20}]}
{"operation": "replace", "collection" : "coll4", "documents": [{"Key1": "vK300", "Field1": 30}]}
{"operation": "update", "collection" : "coll4", "filter" : { "Key1": "vK1" }, "fields" : { "$set" : { "Field1" : 10000 }}}
{"operation": "delete", "collection" : "coll4", "filter" : { "Key1": "vK2" }}
{"operation": "read", "collection" : "coll4"}
EOF

	out=$($cli read --project=db1 coll4)
	exp_out='{"Key1": "vK1", "Field1": 10000}
{"Key1": "vK200", "Field1": 20}
{"Key1": "vK300", "Field1": 30}'
	diff -w -u <(echo "$exp_out") <(echo "$out")

  db_branch_tests

	db_negative_tests
	db_errors_tests
	db_generate_schema_test

	$cli drop collection --project=db1 coll1 coll2 coll3 coll4 coll5 coll6 coll7 coll111
	$cli delete-project -f db1
}

db_branch_tests() {
  $cli drop collection --project=db1 coll_br1 || true

	echo '[{ "title" : "coll_br1", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" } }, "primary_key": ["Key1"] }]' | $cli create collection --project=db1 -

	main_exp_out='{"Key1": "vK1", "Field1": 1}
{"Key1": "vK2", "Field1": 10}'
	$cli insert --project=db1 coll_br1 '{"Key1": "vK1", "Field1": 1}' '{"Key1": "vK2", "Field1": 10}'

	$cli branch list --project=db1 | grep br1 && exit 1

  $cli branch --project=db1 create br1

	$cli branch list --project=db1 | grep br1

  # data exists outside of the branch
	out=$($cli read --project=db1 coll_br1)
	diff -w -u <(echo "$main_exp_out") <(echo "$out")

	branch_exp_out='{"Key1": "vK1br1", "Field1": 1000}
{"Key1": "vK2br1", "Field1": 10000}'

  (
    # shellcheck disable=SC2030,SC2031
    export TIGRIS_BRANCH=br1
    $cli config show|grep "branch: br1"

    # no data in the branch
    out=$($cli read --project=db1 coll_br1)
	  exp_out=''
    diff -w -u <(echo "$exp_out") <(echo "$out")

	  $cli insert --project=db1 coll_br1 '{"Key1": "vK1br1", "Field1": 1000}' '{"Key1": "vK2br1", "Field1": 10000}'

    # branch sees it's own data
    out=$($cli read --project=db1 coll_br1)
	  diff -w -u <(echo "$branch_exp_out") <(echo "$out")

	  # test branch command line parameter
    out=$($cli read --project=db1 --branch=br1 coll_br1)
	  diff -w -u <(echo "$branch_exp_out") <(echo "$out")
  )

  # insert more data in the main
	add_main_exp_out='{"Key1": "vK3", "Field1": 1}'
	$cli insert --project=db1 coll_br1 "$add_main_exp_out"

  (
    # shellcheck disable=SC2030,SC2031
    export TIGRIS_BRANCH=br1
    $cli config show|grep "branch: br1"
    [ "$($cli branch show)" == "br1" ]

    # branch data intact after data insert in the main
    out=$($cli read --project=db1 coll_br1)
	  diff -w -u <(echo "$branch_exp_out") <(echo "$out")
  )

  # branch changes do not affect main branch
	out=$($cli read --project=db1 coll_br1)
	diff -w -u <(echo -e "$main_exp_out\n$add_main_exp_out") <(echo "$out")

  $cli branch --project=db1 delete br1

  # main branch data is intact after deleting the branch
  out=$($cli read --project=db1 coll_br1)
	diff -w -u <(echo -e "$main_exp_out\n$add_main_exp_out") <(echo "$out")

  $cli drop collection --project=db1 coll_br1
}

db_negative_tests() {
	#broken json
	echo '{"Key1": "vK10", "Fiel' | $cli insert --project=db1 coll1 - && exit 1
	$cli insert db1 coll1 '{"Key1": "vK10", "Fiel' && exit 1
	#broken array
	echo '[{"Key1": "vK10", "Field1": 10}' | $cli insert --project=db1 coll1 - && exit 1
	$cli insert db1 coll1 '[{"Key1": "vK10", "Field1": 10}' && exit 1

	#not enough arguments
	$cli read "--project=db1" && exit 1
	$cli update "--project=db1" "coll1" '{"Key1": "vK1"}' && exit 1
	$cli replace --project=db1 && exit 1
	$cli insert --project=db1 && exit 1
	$cli delete --project=db1 coll1 && exit 1
	$cli create collection && exit 1
	$cli create project && exit 1
	$cli drop collection --project=db1 && exit 1
	$cli delete-project && exit 1
	$cli list collections && exit 1 # project not set should error out
	true
}

error() {
	exp_out=$1
	shift
	out=$(set +x; "$@" 2>&1 || true)
	out=${out//warning: GOCOVERDIR not set, no coverage data emitted /}
	diff -u <(echo "$exp_out") <(echo "$out")
}

# shellcheck disable=SC2086
db_errors_tests() {
	$cli list projects

	error "project doesn't exist 'db2'" $cli delete-project -f db2

	error "project doesn't exist 'db2'" $cli drop collection --project=db2 coll1

	error "project doesn't exist 'db2'" $cli create collection --project=db2 \
		'{ "title" : "coll1", "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" }, "Field2": { "type": "integer" } }, "primary_key": ["Key1"] }'

	error "project doesn't exist 'db2'" $cli list collections --project=db2

	error "project doesn't exist 'db2'" $cli insert --project=db2 coll1 '{}'

	error "project doesn't exist 'db2'" $cli read --project=db2 coll1 '{}' ||
	error "404 Not Found" $cli read --project=db2 coll1 '{}'

	error "project doesn't exist 'db2'" $cli update --project=db2 coll1 '{}' '{}'

	error "project doesn't exist 'db2'" $cli delete --project=db2 coll1 '{}'

	$cli create project db2
	error "collection doesn't exist 'coll1'" $cli insert --project=db2 coll1 '{}'

	error "collection doesn't exist 'coll1'" $cli read --project=db2 coll1 '{}' ||
	error "404 Not Found" $cli read --project=db2 coll1 '{}'

	error "collection doesn't exist 'coll1'" $cli update --project=db2 coll1 '{}' '{}'

	error "collection doesn't exist 'coll1'" $cli delete --project=db2 coll1 '{}'

	error "schema name is missing" $cli create collection --project=db1 \
		'{ "properties": { "Key1": { "type": "string" }, "Field1": { "type": "integer" }, "Field2": { "type": "integer" } }, "primary_key": ["Key1"] }'

	$cli delete-project -f db2
}

db_generate_schema_test() {
	TIGRIS_LOG_LEVEL=debug $cli generate sample-schema --project sampledb --create
	$cli delete-project -f sampledb
}

BASEDIR=$(dirname "$0")
# shellcheck disable=SC1091,SC1090
source "$BASEDIR/import.sh"
# shellcheck disable=SC1091,SC1090
source "$BASEDIR/backup.sh"
# shellcheck disable=SC1091,SC1090
source "$BASEDIR/scaffold.sh"
# shellcheck disable=SC1091,SC1090
source "$BASEDIR/search/import.sh"

main() { 
	test_config

	# Exercise tests via HTTP
	unset TIGRIS_PROTOCOL
	export TIGRIS_URL="localhost:$TIGRIS_TEST_PORT"
	db_tests
	test_import

	test_search_import
	test_backup

	if [ -z "$TIGRIS_CLI_TEST_FAST" ]; then
		test_scaffold
	fi

	# Exercise tests via GRPC
	export TIGRIS_URL="localhost:$TIGRIS_TEST_PORT"
	export TIGRIS_PROTOCOL=grpc
	$cli config show | grep "protocol: grpc"
	$cli config show | grep "url: localhost:$TIGRIS_TEST_PORT"
	db_tests
	test_import
	test_backup

	export TIGRIS_URL="localhost:$TIGRIS_TEST_PORT"
	export TIGRIS_PROTOCOL=http
	$cli config show | grep "protocol: http"
	$cli config show | grep "url: localhost:$TIGRIS_TEST_PORT"
	db_tests

	export TIGRIS_PROTOCOL=grpc
	export TIGRIS_URL="http://localhost:$TIGRIS_TEST_PORT"
	$cli config show | grep "protocol: grpc"
	$cli config show | grep "url: http://localhost:$TIGRIS_TEST_PORT"
	db_tests

	export TIGRIS_PROTOCOL=http
	export TIGRIS_URL="grpc://localhost:$TIGRIS_TEST_PORT"
	$cli config show | grep "protocol: http"
	$cli config show | grep "url: grpc://localhost:$TIGRIS_TEST_PORT"
	db_tests
}

main

if [ -z "$noup" ]; then
	$cli local down
fi

test_dev_alias() {
	port=9083

	$cli dev start $port

	export TIGRIS_URL=http://localhost:$port
	$cli config show | grep "protocol: http"
	$cli config show | grep "url: http://localhost:$port"
	db_tests

	$cli dev stop $port
}

if [ -z "$noup" ]; then
	test_dev_alias
fi

