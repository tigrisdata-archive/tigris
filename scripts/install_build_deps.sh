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

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.7.3
#generate openapi 3.0 spec
go install github.com/google/gnostic/cmd/protoc-gen-openapi@v0.6.6
#generate go http client
go install github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.9.1

FDB_VERSION=6.3.23
FDB_SHA512=06ae7fc9e404118f0a707094eafe709e7a918483e8694abe7e601c1e46d1fdeed9fd9c538d8a8252f50f284a573dd735ba81ab54d9aba65b00690b8be90f0b43
wget https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/foundationdb-clients_$FDB_VERSION-1_amd64.deb
echo "$FDB_SHA512 foundationdb-clients_$FDB_VERSION-1_amd64.deb" | sha512sum -c
dpkg -i foundationdb-clients_$FDB_VERSION-1_amd64.deb # provides /lib/libfdb_c.so shared libarry in the docker for CGO

