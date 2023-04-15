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


# This intended to be run duing Docker build only

set -e

VERSION=1.20.3
ARCH=$(dpkg --print-architecture)
FN="go${VERSION}.linux-${ARCH}.tar.gz"

case "$ARCH" in
"amd64")
  SHA256="979694c2c25c735755bf26f4f45e19e64e4811d661dd07b8c010f7a8e18adfca"
  ;;
"arm64")
  SHA256="eb186529f13f901e7a2c4438a05c2cd90d74706aaa0a888469b2a4a617b6ee54"
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

export PATH=$PATH:/usr/local/go/bin

