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


set -e

IN_FILE=$1
OUT_FILE=$2

fix_bytes() {
	# According to the OpenAPI spec format should be "byte",
	# but protoc-gen-openapi generates it as "bytes".
	# We fix it here
	# This is done last to also copy input file to output
	sed -e 's/format: bytes/format: byte/g' "$IN_FILE" >"$OUT_FILE"
}

if [[ "$OUT_FILE" != *"api_openapi"* ]]; then
	fix_bytes
	exit 0
fi

yq_cmd() {
	yq -I 4 -i "$1" "$IN_FILE"
}

# Change type of documents, filters, fields, schema to be JSON object
# instead of bytes.
# It's defined as bytes in proto to implement custom unmarshalling.
yq_fix_object() {
	yq_cmd "del(.components.schemas.$1.properties.$2.format)"
	yq_cmd ".components.schemas.$1.properties.$2.type=\"object\""
}

# Delete db and collection fields from request body
yq_del_db_coll() {
	yq_cmd "del(.components.schemas.$1.properties.db)"
	yq_cmd "del(.components.schemas.$1.properties.collection)"
}

# Fix the types of filter and document fields to be object on HTTP wire.
# The original format in proto file is "bytes", which allows to skip
# unmarshalling in GRPC, we also implement custom unmashalling for HTTP
for i in DeleteRequest UpdateRequest ReadRequest; do
	yq_fix_object $i filter
done

yq_fix_object InsertRequest documents.items
yq_fix_object ReplaceRequest documents.items
yq_fix_object UpdateRequest fields
yq_fix_object ReadResponse doc
yq_fix_object CreateOrUpdateCollectionRequest schema

for i in InsertRequest ReplaceRequest UpdateRequest DeleteRequest ReadRequest \
	CreateOrUpdateCollectionRequest DropCollectionRequest \
	CreateDatabaseRequest DropDatabaseRequest \
	ListDatabasesRequest ListCollectionsRequest \
	BeginTransactionRequest CommitTransactionRequest RollbackTransactionRequest; do

	yq_del_db_coll $i
done

fix_bytes

