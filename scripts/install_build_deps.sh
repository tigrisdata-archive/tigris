#!/usr/bin/env bash
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

# Settings
FDB_VERSION=7.1.7

### Prereqs checks ###
# Check if architecture and OS is supported
# and set environment specifics
ARCH=$(uname -m)
OS=$(uname -s)
case "${OS}-${ARCH}" in
	"Darwin-arm64")
		BINARIES="brew curl go"
		FDB_SHA=b456a1d03580f81394502e1b066006ec38bf6a3a17a9904e6d7a88badbea4b4a08b9dbf69fbb0057b46dd5043f23de9ec3ff7a77d767c23f872425eb73fdee18
		;;
	"Darwin-x86_64")
		BINARIES="brew curl go"
		FDB_SHA=0e9d147410eede58d121fdc9208ea7b04a7c74c8f3776f56cfc00233cfb3a358a0e2f992122718ef9c639928b081801da542e9c4b07a539c8fd73361ab43beec
		;;
	"Linux-aarch64")
		BINARIES="apt-get curl go"
		FDB_SHA=8173952f0aa8dabfc7da9cb23b8eff4de08831a02a3fce846fc7f996f0d3bed33588caf6f7d837ffde71ecf4b512e5e17e3af7e4c52acf838f10b3131656274e
		;;
	"Linux-arm64")
		BINARIES="apt-get curl go"
		FDB_SHA=8173952f0aa8dabfc7da9cb23b8eff4de08831a02a3fce846fc7f996f0d3bed33588caf6f7d837ffde71ecf4b512e5e17e3af7e4c52acf838f10b3131656274e
		;;
	"Linux-x86_64")
		BINARIES="apt-get curl go"
		FDB_SHA=03b1b0705ae8297aa5da8ff7ae2f982208a25a7c6604c4029ff37b19856136528092788ea93b9ffc4deea604b07ab863cdc6f8152f76133ad04aba8794041185
		;;
	*)
		echo "Unsupported architecture ${ARCH} or operating system ${OS}."
		exit 1
esac

# Check if required binaries are available in PATH
for bin in $(echo "${BINARIES}"); do
	binpath=$(which "${bin}")
	if [ -z "${binpath}" ] || ! test -x "${binpath}"; then
		echo "Please ensure that $bin binary is installed and in PATH."
		exit 1
	fi
done

# Install protobuf compiler via package manager
case "${OS}" in
	"Darwin")
		brew install protobuf
		;;
	"Linux")
		sudo apt-get install -y protobuf-compiler
		;;
esac

# Install protobuf
export GO111MODULE=on
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2
go install github.com/google/gnostic/cmd/protoc-gen-openapi@v0 #generate openapi 3.0 spec
go install github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1 #generate go http client
go install github.com/mikefarah/yq/v4@latest # used to fix OpenAPI spec in scripts/fix_openapi.sh

# Install FoundationDB package
case "${OS}" in
	"Darwin")
		FDB_PACKAGE_NAME="FoundationDB-${FDB_VERSION}_${ARCH}.pkg"
		FDB_PACKAGE_PATH="$(mktemp -d)/${FDB_PACKAGE_NAME}"
		curl --create-dirs -Lo "$FDB_PACKAGE_PATH" "https://tigrisdata-pub.s3.us-west-2.amazonaws.com/${FDB_PACKAGE_NAME}"
		echo "$FDB_SHA  $FDB_PACKAGE_PATH" | shasum -a 512 -c
		sudo installer -pkg "$FDB_PACKAGE_PATH" -target /
		;;
	"Linux")
		FDB_PACKAGE_NAME="FoundationDB-${FDB_VERSION}_${ARCH}.deb"
		FDB_PACKAGE_PATH="$(mktemp -p /tmp/ -u)/${FDB_PACKAGE_NAME}"
		curl --create-dirs -Lo "$FDB_PACKAGE_PATH" "https://tigrisdata-pub.s3.us-west-2.amazonaws.com/${FDB_PACKAGE_NAME}"
		echo "$FDB_SHA  $FDB_PACKAGE_PATH" | sha512sum -c
		sudo dpkg -i "$FDB_PACKAGE_PATH" # provides /lib/libfdb_c.so shared library in the docker for CGO
		;;
esac
