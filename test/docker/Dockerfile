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

FROM tigrisdata/tigris-build-base:latest

COPY go.mod /go/src/tigris/
COPY test/docker/test-token-rsa.jwt /etc/test-token-rsa.jwt
COPY test/docker/test-token-hs.jwt /etc/test-token-hs.jwt
COPY test/docker/test-token-A-hs.jwt /etc/test-token-A-hs.jwt
COPY test/docker/test-token-B-hs.jwt /etc/test-token-B-hs.jwt

WORKDIR /go/src/tigris

RUN GO111MODULE=on go mod download

RUN rm -rf ./*

CMD make local_test
