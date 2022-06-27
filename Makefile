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
DOCKER_DIR=test/docker
DOCKER_COMPOSE=COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f ${DOCKER_DIR}/docker-compose.yml
CGO_ENABLED=1

all: server

.PRECIOUS: ${PROTO_DIR}/%_openapi.yaml ${PROTO_DIR}/%.proto

BUILD_PARAM=-tags=release -ldflags "-X 'github.com/tigrisdata/tigris/util.Version=$(VERSION)' -X 'github.com/tigrisdata/tigris/util.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test,integration $(shell printenv TEST_PARAM)

${PROTO_DIR}/%.proto:
	git submodule update --init --recursive

# Generate GRPC client/server, openapi spec, http server
${PROTO_DIR}/%_openapi.yaml ${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${PROTO_DIR}/%.proto
	make -C api/proto generate GEN_DIR=../../${GEN_DIR} API_DIR=..

${DATA_PROTO_DIR}/%.pb.go: ${DATA_PROTO_DIR}/%.proto
	protoc -I${DATA_PROTO_DIR} --go_out=${DATA_PROTO_DIR} --go_opt=paths=source_relative $<

generate: ${GEN_DIR}/api.pb.go ${GEN_DIR}/api.pb.gw.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/health.pb.gw.go ${DATA_PROTO_DIR}/data.pb.go

server: server/service
server/service: $(GO_SRC) generate
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_PARAM) -o server/service ./server

lint: generate
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yaml config/*.yaml
	shellcheck scripts/*
	shellcheck test/docker/grafana/*
	golangci-lint run

docker_compose_build:
	$(DOCKER_COMPOSE) build

# dependency on generate needed to create generated file outside of docker with
# current user owner instead of root
docker_test: generate
	$(DOCKER_COMPOSE) up --build --abort-on-container-exit --exit-code-from tigris_test tigris_test

docker_test_no_build:
	$(DOCKER_COMPOSE) up --no-build --abort-on-container-exit --exit-code-from tigris_test tigris_test

test: clean docker_test

# Use this target to run the test from inside docker container
local_test: generate
	go test $(TEST_PARAM) ./...

run: clean generate
	$(DOCKER_COMPOSE) up --build --detach tigris_server2

local_run: server
	$(DOCKER_COMPOSE) up --no-build --detach tigris_search tigris_fdb
	fdbcli -C ./test/config/fdb.cluster --exec "configure new single memory"
	TIGRIS_ENVIRONMENT=dev ./server/service

# Runs tigris server and foundationdb, plus additional tools for it like:
# - prometheus and grafana for monitoring
run_full: clean generate
	${DOCKER_COMPOSE} up --build --detach tigris_grafana
	./${DOCKER_DIR}/grafana/set_admin_password.sh
	./${DOCKER_DIR}/grafana/add_prometheus_datasource.sh

bins: $(BINS)

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -f server/service api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \

upgrade_api:
	git submodule update --remote --recursive --rebase
