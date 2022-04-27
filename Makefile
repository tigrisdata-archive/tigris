BINS=server
VERSION=$(shell git describe --tags --always)
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}
PROTO_DIR=${API_DIR}/proto/server/${V}
DATA_PROTO_DIR=internal

# Needed to be able to build amd64 binaries on MacOS M1
DOCKER_PLATFORM="linux/amd64"
DOCKER_COMPOSE=COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM=$(DOCKER_PLATFORM) docker-compose -f test/docker/docker-compose.yml
GOARCH="amd64"
CGO_ENABLED=1

OSX_CLUSTER_FILE="/usr/local/etc/foundationdb/fdb.cluster"

all: server

BUILD_PARAM=-tags=release -ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test,integration $(shell printenv TEST_PARAM)

${PROTO_DIR}/%.proto:
	git submodule update --init --recursive

# Generate GRPC client/server, openapi spec, http server
${PROTO_DIR}/%_openapi.yaml ${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${PROTO_DIR}/%.proto
	protoc -Iapi/proto --openapi_out=${API_DIR} --openapi_opt=naming=proto \
		--go_out=${API_DIR} --go_opt=paths=source_relative \
		--go-grpc_out=${API_DIR} --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=${API_DIR} --grpc-gateway_opt=paths=source_relative,allow_delete_body=true \
		$<
	/bin/bash scripts/fix_openapi.sh ${API_DIR}/openapi.yaml ${PROTO_DIR}/$(*F)_openapi.yaml
	rm ${API_DIR}/openapi.yaml 

# Generate Go HTTP client from openapi spec
${API_DIR}/client/${V}/%/http.go: ${PROTO_DIR}/%_openapi.yaml
	mkdir -p ${API_DIR}/client/${V}/$(*F)
	oapi-codegen -package api -generate "client, types, spec" \
		-o ${API_DIR}/client/${V}/$(*F)/http.go \
		${PROTO_DIR}/$(*F)_openapi.yaml

${DATA_PROTO_DIR}/%.pb.go ${DATA_PROTO_DIR}/%.pb.gw.go: ${DATA_PROTO_DIR}/%.proto
	protoc -Iinternal \
		--go_out=${DATA_PROTO_DIR} --go_opt=paths=source_relative \
		$<

generate: ${GEN_DIR}/api.pb.go ${GEN_DIR}/api.pb.gw.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/health.pb.gw.go ${DATA_PROTO_DIR}/data.pb.gw.go

test_client: ${API_DIR}/client/${V}/api/http.go

server: server/service
server/service: $(GO_SRC) generate
	GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_PARAM) -o server/service ./server

lint: generate test_client
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yaml config/*.yaml
	shellcheck scripts/*
	golangci-lint run

docker_compose_build:
	$(DOCKER_COMPOSE) build

# dependency on generate needed to create generated file outside of docker with
# current user owner instead of root
docker_test: generate test_client
	$(DOCKER_COMPOSE) up --build --abort-on-container-exit --exit-code-from tigris_test tigris_test

docker_test_no_build:
	$(DOCKER_COMPOSE) up --no-build --abort-on-container-exit --exit-code-from tigris_test tigris_test

test: clean docker_test

# Use this target to run the test from inside docker container
local_test: generate test_client
	go test $(TEST_PARAM) ./...

run: clean generate
	$(DOCKER_COMPOSE) up --build --detach tigris_server

bins: $(BINS)

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -f server/service api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \
		api/server/${V}/*_openapi.yaml \
		api/client/${V}/*/http.go

# OSX specific targets to run tests against FDB installed on the Mac OSX host (non-containerized)
osx_test: generate test_client
	TIGRIS_SERVER_FOUNDATIONDB_CLUSTER_FILE=$(OSX_CLUSTER_FILE) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED)  go test $(TEST_PARAM) ./...

osx_run: generate server
	TIGRIS_SERVER_FOUNDATIONDB_CLUSTER_FILE=$(OSX_CLUSTER_FILE) ./server/service

upgrade_api:
	git submodule update --remote --recursive --rebase

