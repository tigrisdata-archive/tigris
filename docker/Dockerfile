FROM golang:1.18-bullseye AS build

RUN apt-get update && apt-get install -y --no-install-recommends apt-transport-https sudo

RUN mkdir /build

#Download deps once, during docker build. Rebuild only on go.mod change
COPY scripts/install_build_deps.sh /build
RUN sh /build/install_build_deps.sh
COPY go.mod /build
WORKDIR /build
RUN go mod download

COPY . /build

RUN rm -f server/service && make bins

FROM debian:bullseye-slim

# Remove apt configuration
RUN rm -rf /etc/apt/*

# Setup an unprivileged user
RUN groupadd -r tigris && useradd -r -s /bin/false -g tigris tigris

RUN mkdir -p /server /etc/tigrisdata/tigrisdb /etc/foundationdb/

COPY --from=build /build/server/service /server/service
COPY --from=build /build/config/server.yaml /etc/tigrisdata/tigrisdb
COPY --from=build /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so

RUN chown -R tigris:tigris /server /etc/tigrisdata/tigrisdb

EXPOSE 8081

WORKDIR /server

# Set the default effective user to an unprivileged user
USER tigris

CMD ["/server/service"]
