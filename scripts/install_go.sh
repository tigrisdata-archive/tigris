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


# This intended to be run duing Docker build only

set -e

ARCH=$(dpkg --print-architecture)

mkdir -p /usr/local
wget "https://go.dev/dl/go1.18.3.linux-${ARCH}.tar.gz"
tar -C /usr/local -xzf "go1.18.3.linux-${ARCH}.tar.gz"
rm "go1.18.3.linux-${ARCH}.tar.gz"
export PATH=$PATH:/usr/local/go/bin

