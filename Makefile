BINS=server
VERSION=1.0.0
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
ESRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}

all: server

BUILD_PARAM=-tags=release -ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test $(shell printenv TEST_PARAM)

# generate GRPC client/server, openapi spec, http server
${GEN_DIR}/%_openapi.yaml ${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${GEN_DIR}/%.proto
	protoc -Iapi --openapi_out=${API_DIR} --openapi_opt=naming=proto \
		--go_out=${API_DIR} --go_opt=paths=source_relative \
		--go-grpc_out=${API_DIR} --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=${API_DIR} --grpc-gateway_opt=paths=source_relative \
		$<
	sed -i'' -e 's/format: bytes/format: byte/g' ${API_DIR}/openapi.yaml
	mv ${API_DIR}/openapi.yaml ${GEN_DIR}/$(*F)_openapi.yaml

# generate Go HTTP client from openapi spec
${API_DIR}/client/${V}/%/http.go: ${GEN_DIR}/%_openapi.yaml
	mkdir -p ${API_DIR}/client/${V}/$(*F)
	oapi-codegen -package api -generate "client, types, spec" \
		-o ${API_DIR}/client/${V}/$(*F)/http.go \
		${GEN_DIR}/$(*F)_openapi.yaml
# generate client for other languagaes
#	docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
#    -i /local/${GEN_DIR}/$(*F)_openapi.yaml -g go \
#    -o /local/${GEN_DIR}/out
#

gen_proto: ${GEN_DIR}/user.pb.go ${GEN_DIR}/user.pb.gw.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/health.pb.gw.go

test_client: ${API_DIR}/client/${V}/user/http.go

server: server/service
server/service: $(ESRC) gen_proto
	go build $(BUILD_PARAM) -o server/service ./server

lint:
	shellcheck scripts/*
	# golangci-lint run #FIXME: doesn't work with go1.18beta1

docker_test:
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.yml up --build --abort-on-container-exit --exit-code-from all_test all_test

test: docker_test

local_test: lint gen_proto test_client
	go test $(TEST_PARAM) ./...

run: $(BINS)
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.yml up --build tigris_server

bins: $(BINS)

clean:
	docker-compose -f docker/docker-compose.yml down -v --remove-orphans
	rm -f server/service api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \
		api/server/${V}/*_openapi.yaml \
		api/client/${V}/*/http.go
