#!/bin/bash

set -ex

cli=./tigris
export cli

CN=test-docker-local

curl -sSL https://tigris.dev/cli-linux | tar -xz -C .

make docker-local

PORT=9981
TMPDIR=/tmp/test_docker_local

export TIGRIS_URL=localhost:$PORT
export TIGRIS_TEST_PORT=$PORT
export TIGRIS_CLI_TEST_FAST=1
export TIGRIS_SKIP_LOCAL_TLS=true
export TIGRIS_NO_TEST_CONFIG=1
export TIGRIS_PROTOCOL=http
export TIGRIS_LOCAL_GENERATE_ADMIN_TOKEN=1

wait_up() {
  TIGRIS_LOG_LEVEL=debug $cli ping --timeout 30s
}

stop() {
  docker stop $CN || true
  docker rm --force $CN || true
}

test_ephemeral() {
  echo -e "\nTest ephemeral instance (started)"

  stop

  docker run --name $CN -d -p $PORT:8081 tigris_local

  wait_up

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli create project test_docker_local

  stop

  docker run --name $CN -d -p $PORT:8081 tigris_local

  wait_up

  $cli list projects | grep test_docker_local && exit 1

  echo -e "Test ephemeral instance (finished)"
}

test_persistent() {
  echo -e "\nTest persistent instance (started)"

  stop

  $SUDO rm -rf $TMPDIR
  mkdir -p $TMPDIR

  docker run --name $CN -e TIGRIS_LOCAL_PERSISTENCE=1 -v $TMPDIR:/var/lib/tigris -d -p $PORT:8081 tigris_local

  wait_up

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli create project test_docker_local

  stop

  docker run --name $CN -v $TMPDIR:/var/lib/tigris -d -p $PORT:8081 tigris_local

  wait_up

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli list projects | grep test_docker_local

  echo -e "\nTest persistent instance (finished)"
}

test_auth() {
  echo -e "\nTest persistent authenticated instance (started)"

  stop

  $SUDO rm -rf $TMPDIR
  mkdir -p $TMPDIR

  docker run -e TIGRIS_BOOTSTRAP_LOCAL_AUTH=1 \
    -e TIGRIS_LOCAL_PERSISTENCE=1 \
    -e TIGRIS_LOCAL_GENERATE_ADMIN_TOKEN=1 \
    --name $CN -v $TMPDIR:/var/lib/tigris \
    -d -p $PORT:8081 tigris_local

  wait_up

  TIGRIS_TOKEN=$(cat /tmp/test_docker_local/user_admin_token.txt)
  export TIGRIS_TOKEN

  wait_up # wait for gotrue

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli create project test_docker_local

  stop

  # Restart the instance with auth
  docker run --name $CN -v $TMPDIR:/var/lib/tigris -d -p $PORT:8081 tigris_local

  wait_up

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli list projects | grep test_docker_local

  unset TIGRIS_TOKEN

  $cli list projects | grep test_docker_local && exit 1 # unauthenticated

  # container owner automatically authenticated on unix socket connection
  $SUDO TIGRIS_PROTOCOL=http TIGRIS_URL=$TMPDIR/server/unix.sock $cli list projects | grep test_docker_local
  $SUDO TIGRIS_PROTOCOL=grpc TIGRIS_URL=$TMPDIR/server/unix.sock $cli list projects | grep test_docker_local

  stop

  # Test unauthenticated existing instance
  docker run -e TIGRIS_SKIP_LOCAL_AUTH=1 \
    --name $CN -v $TMPDIR:/var/lib/tigris -d -p $PORT:8081 tigris_local

  wait_up

  noup=1 /bin/bash test/v1/cli/main.sh

  $cli list projects | grep test_docker_local

  echo "Test persistent authenticated instance (finished)"
}

stop

test_ephemeral
test_persistent
test_auth
