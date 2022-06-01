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

export GO111MODULE=on

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2
go install github.com/google/gnostic/cmd/protoc-gen-openapi@v0 #generate openapi 3.0 spec
go install github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1 #generate go http client
go install github.com/mikefarah/yq/v4@latest # used to fix OpenAPI spec in scripts/fix_openapi.sh

FDB_VERSION=6.3.23
if [[ "$OSTYPE" == "darwin"* ]]; then
	if command -v brew > /dev/null 2>&1; then
		brew install protobuf
	fi

	FDB_PACKAGE_PATH="$(mktemp -d)/FoundationDB-$FDB_VERSION.pkg"
	FDB_SHA512=d7b89e82dae332af09637543371c58bcaaab2c818a3ea49f56e22587d1a6adfc255e154e6c4feca90f407e37d63d8a3cd2e7cfa0b996c2865c9d74fd5dc1b0ba
	curl -Lo "$FDB_PACKAGE_PATH" "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/FoundationDB-${FDB_VERSION}.pkg"
	echo "$FDB_SHA512  $FDB_PACKAGE_PATH" | shasum -a 512 -c
	sudo installer -pkg "$FDB_PACKAGE_PATH" -target /

else
	FDB_PACKAGE_PATH="$(mktemp -p /tmp/ -u)-foundationdb-clients_$FDB_VERSION-1_amd64.deb"
	FDB_SHA512=06ae7fc9e404118f0a707094eafe709e7a918483e8694abe7e601c1e46d1fdeed9fd9c538d8a8252f50f284a573dd735ba81ab54d9aba65b00690b8be90f0b43
	wget https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/foundationdb-clients_$FDB_VERSION-1_amd64.deb -O "$FDB_PACKAGE_PATH"
	echo "$FDB_SHA512 $FDB_PACKAGE_PATH" | sha512sum -c
	sudo dpkg -i "$FDB_PACKAGE_PATH" # provides /lib/libfdb_c.so shared library in the docker for CGO
	sudo apt-get install -y protobuf-compiler
fi
