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

FROM tigrisdata/tigris-build-base AS build

COPY go.mod /build
WORKDIR /build
RUN go mod download
COPY . /build

# This is used to compile with integration coverage enabled
ARG BUILD_PARAM_ARG

RUN --mount=type=cache,target=/root/.cache/go-build rm -f server/service && BUILD_PARAM=${BUILD_PARAM_ARG} make bins


FROM ubuntu:20.04 AS server

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /etc/apt/* /var/lib/dpkg/* /var/lib/apt/* 

# Setup an unprivileged user
RUN groupadd -r tigris && useradd -r -s /bin/false -g tigris tigris

RUN mkdir -p /server /etc/tigrisdata/tigris /etc/foundationdb/

COPY --from=build /build/server/service /server/service
COPY --from=build /build/config/server.yaml /etc/tigrisdata/tigris
COPY --from=build /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=build /usr/bin/fdbcli /usr/bin/fdbcli

RUN chown -R tigris:tigris /server /etc/tigrisdata/tigris

EXPOSE 8081

WORKDIR /server

# Set the default effective user to an unprivileged user
USER tigris

CMD ["/server/service"]
