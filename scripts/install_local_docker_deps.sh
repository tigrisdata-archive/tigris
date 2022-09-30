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

ARCH=$(dpkg --print-architecture)

case "${ARCH}" in
	"arm64")
		FDB_SHA=c994ebb01a660cff9ef699a0e38482a561679db04c02c256efae25ba687cf903
		;;
	"aarch64")
		FDB_SHA=c994ebb01a660cff9ef699a0e38482a561679db04c02c256efae25ba687cf903
		;;
	"amd64")
		FDB_SHA=471f6bf4a7af40abc69027aa0d4f452ee83715a43a555008303ca255f6bd6db1
		;;
esac

FDB_PACKAGE_NAME="foundationdb-clients_7.1.7-1_${ARCH}.deb"
FDB_PACKAGE_PATH="$(mktemp -p /tmp/ -u)/${FDB_PACKAGE_NAME}"
curl --create-dirs -Lo "$FDB_PACKAGE_PATH" "https://tigrisdata-pub.s3.us-west-2.amazonaws.com/ubuntu/focal/${FDB_PACKAGE_NAME}"
echo "$FDB_SHA $FDB_PACKAGE_PATH" | sha256sum -c
dpkg -i "$FDB_PACKAGE_PATH"
rm -f "$FDB_PACKAGE_PATH"

case "${ARCH}" in
	"arm64")
		FDB_SHA=be679eb18ef618251547dd10ef1b25dd2a371006f6069b1cb801649cff96b07e
		;;
	"aarch64")
		FDB_SHA=be679eb18ef618251547dd10ef1b25dd2a371006f6069b1cb801649cff96b07e
		;;
	"amd64")
		FDB_SHA=ff2bfd672637db550267126657022186d383d64783461267ba33619d6fe5397a
		;;
esac

FDB_PACKAGE_NAME="foundationdb-server_7.1.7-1_${ARCH}.deb"
FDB_PACKAGE_PATH="$(mktemp -p /tmp/ -u)/${FDB_PACKAGE_NAME}"
curl --create-dirs -Lo "$FDB_PACKAGE_PATH" "https://tigrisdata-pub.s3.us-west-2.amazonaws.com/ubuntu/focal/${FDB_PACKAGE_NAME}"
echo "$FDB_SHA $FDB_PACKAGE_PATH" | sha256sum -c
dpkg --unpack "$FDB_PACKAGE_PATH"
echo exit 0 >/etc/init.d/foundationdb
chmod u+x /etc/init.d/foundationdb
mkdir -p /etc/foundationdb
echo "docker:docker@127.0.0.1:4500" >/etc/foundationdb/fdb.cluster
dpkg --force-confold --configure foundationdb-server
rm -f "$FDB_PACKAGE_PATH"

TS_PACKAGE_NAME="typesense-server-0.23.0-${ARCH}.deb"
TS_PACKAGE_PATH="$(mktemp -p /tmp/ -u)/${TS_PACKAGE_NAME}"
curl --create-dirs -Lo "$TS_PACKAGE_PATH" "https://dl.typesense.org/releases/0.23.0/${TS_PACKAGE_NAME}"

dpkg --unpack "$TS_PACKAGE_PATH"
rm -f /var/lib/dpkg/info/typesense-server.postinst
dpkg --configure typesense-server
sed -i "s/\$API_KEY/ts_dev_key/g" /etc/typesense/typesense-server.ini && \
rm -f "$TS_PACKAGE_PATH"
