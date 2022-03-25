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

FROM golang:1.18-bullseye

RUN apt-get update && apt-get install -y apt-transport-https
RUN apt-get install -y sudo

RUN mkdir -p /go/src/tigris

#this is to install dependencies once, during docker build
COPY scripts/install_build_deps.sh /go/src/tigris
COPY scripts/install_test_deps.sh /go/src/tigris
RUN sh /go/src/tigris/install_build_deps.sh
RUN sh /go/src/tigris/install_test_deps.sh
COPY go.mod /go/src/tigris/

WORKDIR /go/src/tigris

RUN GO111MODULE=on go mod download

RUN rm -rf ./*

CMD make local_test
