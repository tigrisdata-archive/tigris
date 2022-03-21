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

#Set SSH access to private repos
RUN mkdir -p /root/.ssh && ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts
RUN git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
RUN --mount=type=ssh GO111MODULE=on GOPRIVATE=github.com/tigrisdata/ go mod download

RUN rm -rf ./*

CMD make local_test
