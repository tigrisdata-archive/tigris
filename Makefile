BINS=server
VERSION=$(shell git describe --tags --always)
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}
PROTO_DIR=${API_DIR}/proto/server/${V}
DATA_PROTO_DIR=internal
LINT_TIMEOUT=5m

# Needed to be able to build amd64 binaries on MacOS M1
DOCKER_DIR=test/docker
DOCKER_COMPOSE=COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker compose -f ${DOCKER_DIR}/docker-compose.yml
CGO_ENABLED=1

all: server

# Setup local development environment.
setup: deps
	git config core.hooksPath ./.gitconfig/hooks

# Run tests in the docker.
test: docker_test

# Start local Tigris instance in the docker.
run: coverdir
	$(DOCKER_COMPOSE) up --build --detach tigris_server2

# Dump logs of local Tigris instance started by `run`.
logs:
	$(DOCKER_COMPOSE) logs

# Run tests on the host. It expects Tigris to be started by `run` target.
# This target is also used to run the test from inside docker container
local_test: generate lint
	go test $(TEST_PARAM) ./...

# Start local instance with server running on the host.
# This is useful for debugging the server. The process is attachable from IDE.
local_run: server
	$(DOCKER_COMPOSE) up --no-build --detach tigris_search tigris_db2 tigris_cache
	fdbcli -C ./test/config/fdb.cluster --exec "configure new single memory" || true
	./server/service -c config/server.dev.yaml

# Start local instance with server running on the host in realtime mode.
# This is useful for debugging the server. The process is attachable from IDE.
local_rt_run: server
	$(DOCKER_COMPOSE) up --no-build --detach tigris_search tigris_db2 tigris_cache
	fdbcli -C ./test/config/fdb.cluster --exec "configure new single memory" || true
	TIGRIS_SERVER_SERVER_TYPE=realtime ./server/service -c config/server.dev.yaml

# Runs tigris server and foundationdb, plus additional tools for it like:
# - prometheus and grafana for monitoring
run_full: coverdir
	${DOCKER_COMPOSE} up --build --detach tigris_grafana tigris_jaeger
	./${DOCKER_DIR}/grafana/set_admin_password.sh
	./${DOCKER_DIR}/grafana/add_victoriametrics_datasource.sh

lint: generate
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yaml config/*.yaml >/dev/null
	shellcheck scripts/*
	shellcheck test/docker/grafana/*
	golangci-lint --timeout=$(LINT_TIMEOUT) run --fix

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -f server/service api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \

docker_test: coverdir
	$(DOCKER_COMPOSE) up --build tigris_test tigris_test
	@[ $$(docker inspect tigris_test --format='{{.State.ExitCode}}') = "0" ]

# Install development dependencies.
# For use in CI workflows.
deps:
	/bin/bash scripts/install_build_deps.sh
	/bin/bash scripts/install_test_deps.sh


# The following targets are for API code generation.
.PRECIOUS: ${PROTO_DIR}/%_openapi.yaml ${PROTO_DIR}/%.proto

BUILD_PARAM=-tags=release -ldflags "-X 'github.com/tigrisdata/tigris/util.Version=$(VERSION)' -X 'github.com/tigrisdata/tigris/util.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test,integration $(shell printenv TEST_PARAM)

${PROTO_DIR}/%.proto:
	git submodule update --init --recursive

# Generate GRPC client/server, openapi spec, http server.
${PROTO_DIR}/%_openapi.yaml ${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${PROTO_DIR}/%.proto
	make -C api/proto generate GEN_DIR=../../${GEN_DIR} API_DIR=..

${DATA_PROTO_DIR}/%.pb.go: ${DATA_PROTO_DIR}/%.proto
	protoc -I${DATA_PROTO_DIR} --go_out=${DATA_PROTO_DIR} --go_opt=paths=source_relative $<

coverdir:
	mkdir -p /tmp/tigris_coverdata && chmod a+w /tmp/tigris_coverdata; rm -f /tmp/tigris_coverdata/*
	mkdir -p /tmp/tigris_coverdata2 && chmod a+w /tmp/tigris_coverdata2; rm -f /tmp/tigris_coverdata2/*

generate: ${GEN_DIR}/api.pb.go ${GEN_DIR}/api.pb.gw.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/health.pb.gw.go ${GEN_DIR}/admin.pb.go ${GEN_DIR}/admin.pb.gw.go ${DATA_PROTO_DIR}/data.pb.go

# Build the server binary.
server: server/service
server/service: $(GO_SRC) generate
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_PARAM) -o server/service ./server

bins: $(BINS)

# Pull the API submodule changes.
upgrade_api:
	git submodule update --remote --recursive --rebase

# This is used to update https://hub.docker.com/repository/docker/tigrisdata/tigris-build-base
build_and_push_base_docker:
	docker buildx build -t tigrisdata/tigris-build-base:latest --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile.base .

# This is used in CI workflows
dump_integration_coverage:
	pkill -SIGTERM -f "/server/service" --exact
	sleep 15
	/usr/local/go/bin/go tool covdata textfmt -i=/tmp/tigris_coverdata/ -o coverage1.out # from tigris_server
	/usr/local/go/bin/go tool covdata textfmt -i=/tmp/tigris_coverdata2/ -o coverage2.out # from tigris_server2

# Build local all-in-one package
docker-local:
	DOCKER_BUILDKIT=1 docker build -t tigris_local -f docker/Dockerfile.local .
