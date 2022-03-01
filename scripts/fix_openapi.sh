#!/bin/bash

set -e

IN_FILE=$1
OUT_FILE=$2

yq_cmd() {
	yq -I 4 -i "$1" "$IN_FILE"
}

yq_fix_object() {
	yq_cmd "del(.components.schemas.$1.properties.$2.format)"
	yq_cmd ".components.schemas.$1.properties.$2.type=\"object\""
}

yq_fix_object_array() {
	yq_cmd "del(.components.schemas.$1.properties.$2.items.format)"
	yq_cmd ".components.schemas.$1.properties.$2.items.type=\"object\""
}

# Fix the types of filter and document fields to be object on HTTP wire.
# The original format in proto file is "bytes", which allows to skip
# unmarshalling in GRPC, we also implement custom unmashalling for HTTP
for i in DeleteRequest UpdateRequest ReadRequest; do
	yq_fix_object $i filter
done

for i in InsertRequest ReplaceRequest; do
	yq_fix_object_array $i documents
done

yq_fix_object ReadResponse doc

# According to the OpenAPI spec format should be "byte",
# but protoc-gen-openapi generates it as "bytes".
# We fix it here
# This is done last to also copy input file to output
sed -e 's/format: bytes/format: byte/g' "$IN_FILE" >"$OUT_FILE"

