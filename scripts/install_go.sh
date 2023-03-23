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

VERSION=1.20.2
ARCH=$(dpkg --print-architecture)
FN="go${VERSION}.linux-${ARCH}.tar.gz"

case "$ARCH" in
"amd64")
  SHA256="4eaea32f59cde4dc635fbc42161031d13e1c780b87097f4b4234cfce671f1768"
  ;;
"arm64")
  SHA256="78d632915bb75e9a6356a47a42625fd1a785c83a64a643fedd8f61e31b1b3bef"
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

