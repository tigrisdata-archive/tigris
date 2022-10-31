#!/bin/bash
# Copyright 2022-2023 Tigris Data, Inc.
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

# This intended to be run duing Docker build only

set -e

VERSION=1.20
#ARCH=$(uname -m)
OS=$(uname -s)
ARCH=$(dpkg --print-architecture)
FN="go${VERSION}.linux-${ARCH}.tar.gz"

case "$ARCH" in
"amd64")
  SHA256="5a9ebcc65c1cce56e0d2dc616aff4c4cedcfbda8cc6f0288cc08cda3b18dcbf1"
  ;;
"arm64")
  SHA256="17700b6e5108e2a2c3b1a43cd865d3f9c66b7f1c5f0cec26d3672cc131cc0994"
  ;;
*)
  echo "No supported architecture."
  exit 1
  ;;
esac

wget "https://go.dev/dl/$FN"
echo "$SHA256  $FN" | shasum -a 256 -c

mkdir -p /usr/local
tar -C /usr/local -xzf "$FN"
rm "$FN"


if false; then
case "${OS}-${ARCH}" in
"Darwin-arm64")
  V="darwin-arm64.tar.gz"
  SHA="49e394ab92bc6fa3df3d27298ddf3e4491f99477bee9dd4934525a526f3a391c"
  ;;
"Darwin-x86_64")
  V="darwin-amd4.tar.gz"
  SHA="7fa09a9a34cb6f794e61e9ada1d6d18796f936a2b35f22724906cad71396e590"
  ;;
"Linux-arm64")
  V="linux-arm64.tar.gz"
  SHA="99de2fe112a52ab748fb175edea64b313a0c8d51d6157dba683a6be163fd5eab"
  ;;
"Linux-x86_64")
  V="linux-amd64.tar.gz"
  SHA="74b9640724fd4e6bb0ed2a1bc44ae813a03f1e72a4c76253e2d5c015494430ba"
  ;;
"MINGW"*)
  V="windows-amd64.zip"
  SHA="b51549a9f21ee053f8a3d8e38e45b1b8b282d976f3b60f1f89b37ac54e272d31"
  ;;
*)
  echo "Unsupported architecture ${ARCH} or operating system ${OS}."
  exit 1
  ;;
esac

curl -LO "https://go.dev/dl/go${VERSION}.${V}"

if [[ "$OS" == "MINGW"* ]]; then
    unzip "go${VERSION}.${V}" -d "/usr/local/"
else
	tar -C /usr/local -xzf "go${VERSION}.${V}"
fi
fi

export PATH=$PATH:/usr/local/go/bin

