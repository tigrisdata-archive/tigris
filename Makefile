BINS=server
VERSION=1.0.0
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}

# Needed to be able to build amd64 binaries on MacOS M1
DOCKER_PLATFORM="linux/amd64"
DOCKER_COMPOSE=COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM=$(DOCKER_PLATFORM) docker-compose -f test/docker/docker-compose.yml
GOARCH="amd64"
CGO_ENABLED=1

OSX_CLUSTER_FILE="/usr/local/etc/foundationdb/fdb.cluster"

all: server

BUILD_PARAM=-tags=release -ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test,integration $(shell printenv TEST_PARAM)

# Generate GRPC client/server, openapi spec, http server
${GEN_DIR}/%_openapi.yaml ${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${GEN_DIR}/%.proto
	protoc -Iapi --openapi_out=${API_DIR} --openapi_opt=naming=proto \
		--go_out=${API_DIR} --go_opt=paths=source_relative \
		--go-grpc_out=${API_DIR} --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=${API_DIR} --grpc-gateway_opt=paths=source_relative,allow_delete_body=true \
		$<
	/bin/bash scripts/fix_openapi.sh ${API_DIR}/openapi.yaml ${GEN_DIR}/$(*F)_openapi.yaml
	rm ${API_DIR}/openapi.yaml 

# Generate Go HTTP client from openapi spec
${API_DIR}/client/${V}/%/http.go: ${GEN_DIR}/%_openapi.yaml
	mkdir -p ${API_DIR}/client/${V}/$(*F)
	oapi-codegen -package api -generate "client, types, spec" \
		-o ${API_DIR}/client/${V}/$(*F)/http.go \
		${GEN_DIR}/$(*F)_openapi.yaml

generate: ${GEN_DIR}/user.pb.go ${GEN_DIR}/user.pb.gw.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/health.pb.gw.go

test_client: ${API_DIR}/client/${V}/user/http.go

server: server/service
server/service: $(GO_SRC) generate
	GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_PARAM) -o server/service ./server

lint: generate test_client
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yml config/*.yaml
	shellcheck scripts/*
	golangci-lint run

docker_compose_build:
	$(DOCKER_COMPOSE) build

docker_test:
	$(DOCKER_COMPOSE) up --build --abort-on-container-exit --exit-code-from tigris_test tigris_test

docker_test_no_build:
	$(DOCKER_COMPOSE) up --no-build --abort-on-container-exit --exit-code-from tigris_test tigris_test

test: clean docker_test

# Use this target to run the test from inside docker container
local_test: generate test_client
	go test $(TEST_PARAM) ./...

run: clean generate
	$(DOCKER_COMPOSE) up --build tigris_server

bins: $(BINS)

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -f server/service api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \
		api/server/${V}/*_openapi.yaml \
		api/client/${V}/*/http.go

# OSX specific targets to run tests against FDB installed on the Mac OSX host (non-containerized)
osx_test: generate test_client
	TIGRISDB_SERVER_FOUNDATIONDB_CLUSTER_FILE=$(OSX_CLUSTER_FILE) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED)  go test $(TEST_PARAM) ./...

osx_run: generate server
	TIGRISDB_SERVER_FOUNDATIONDB_CLUSTER_FILE=$(OSX_CLUSTER_FILE) ./server/service
