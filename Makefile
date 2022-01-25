BINS=server
VERSION=1.0.0
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
ESRC=$(shell find . -name "*.go" -not -name "*_test.go")
GEN_DIR=api
V=v1

all: server

BUILD_PARAM=-tags=release -ldflags "-extldflags '-static' -X 'main.Version=$(VERSION)' -X 'main.BuildHash=$(GIT_HASH)'"

# generate GRPC client/server, openapi spec, http server
${GEN_DIR}/server/${V}/%_openapi.yaml ${GEN_DIR}/server/${V}/%.pb.go ${GEN_DIR}/server/${V}/%.pb.gw.go: api/server/${V}/%.proto
	protoc -Iapi --openapi_out=${GEN_DIR} --grpc-gateway_out=${GEN_DIR} --go_out=${GEN_DIR} --go-grpc_out=${GEN_DIR} --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --grpc-gateway_opt paths=source_relative $<
	sed -i 's/format: bytes/format: byte/g' ${GEN_DIR}/openapi.yaml
	mv ${GEN_DIR}/openapi.yaml ${GEN_DIR}/server/${V}/$(*F)_openapi.yaml

# generate Go HTTP client from openapi spec
${GEN_DIR}/client/${V}/%/http.go: ${GEN_DIR}/server/${V}/%_openapi.yaml
	mkdir -p ${GEN_DIR}/client/${V}/$(*F)
	oapi-codegen -package api -generate "client, types, spec" -o ${GEN_DIR}/client/${V}/$(*F)/http.go ${GEN_DIR}/server/${V}/$(*F)_openapi.yaml
# generate client for other languagaes
#	docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
#    -i /local/${GEN_DIR}/server/${V}/$(*F)_openapi.yaml -g go \
#    -o /local/${GEN_DIR}/server/${V}/out
#

test_client: ${GEN_DIR}/client/${V}/index/http.go ${GEN_DIR}/client/${V}/user/http.go

server: server/service
server/service: $(ESRC) ${GEN_DIR}/server/${V}/index.pb.go ${GEN_DIR}/server/${V}/index.pb.gw.go ${GEN_DIR}/server/${V}/user.pb.go ${GEN_DIR}/server/${V}/user.pb.gw.go
	CGO_ENABLED=0 go build $(BUILD_PARAM) -o server/service ./server

lint:
	#golangci-lint run

#this is called by docker/Dockerfile.test
test_deps:
	#go install github.com/google/gnostic/cmd/protoc-gen-openapi
	go get github.com/golang/mock/mockgen@v1.6.0
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.43.0

docker_test: $(BINS) test_client
	docker-compose -f docker/docker-compose.yml up --build --abort-on-container-exit --exit-code-from all_test all_test

test: docker_test

local_test: lint
	go test -v -cover $(TEST_PARAM) $(BUILD_PARAM) -tags=test ./...

run: $(BINS)
	docker-compose -f docker/docker-compose.yml up --build server

bins: $(BINS)

clean:
	docker-compose -f docker/docker-compose.yml down -v --remove-orphans
	rm -f server/service
