#!/bin/bash

set -ex

TMP_DIR=/tmp/cli-test
PORT=8080

export TIGRIS_TEMPLATES_PATH=.

if [ -z "$cli" ]; then
  mkdir -p $TMP_DIR/bin
  curl -sSL https://tigris.dev/cli-linux | tar -xz -C $TMP_DIR/bin

  cli="$TMP_DIR/bin/tigris"
fi

$cli config show
env|grep TIGRIS

export TIGRIS_URL=localhost:8081

#if [ -z "$noup" ]; then
#  $cli local up
#fi

# first parameter is path
# second parameter is document to write
request() {
  curl --fail -X POST "localhost:$PORT/$1" -H 'Content-Type: application/json' -d "$2"
  echo
}

test_crud_routes() {
  sleep 3 # give some time server to start

  request "$1"users '{"id":1, "name":"John","balance":100}' #Id=1
  request "$1"users '{"id":2, "name":"Jane","balance":200}' #Id=2

  request "$1"products '{"id":1, "name":"Avocado","price":10,"quantity":5}' #Id=1
  request "$1"products '{"id":2, "name":"Gold","price":3000,"quantity":1}' #Id=2

  #low balance
  request "$1"orders '{"id":1, "user_id":1,"productItems":[{"Id":2,"Quantity":1}]}' || true
  # low stock
  request "$1"orders '{"id":2, "user_id":1,"productItems":[{"Id":1,"Quantity":10}]}' || true

  request "$1"orders '{"id":3, "user_id":1,"productItems":[{"Id":1,"Quantity":5}]}' #Id=1

  curl --fail localhost:$PORT/"$1"user/1
  echo
  curl --fail localhost:$PORT/"$1"product/1
  echo
  curl --fail localhost:$PORT/"$1"order/1
  echo

  # search
  #request "$1"users/search '{"q":"john"}'
  #request "$1"products/search '{"q":"avocado","searchFields": ["name"]}'
  curl --fail "localhost:$PORT/${1}users/search?q=john"
  curl --fail "localhost:$PORT/${1}products/search?q=avocado"
}

start_service() {
  TIGRIS_URL=tigris-local-server:8081 docker compose up -d tigris
  #$cli ping --timeout=20s
  sleep 5
  $cli create project "$db"
  TIGRIS_URL=tigris-local-server:8081 docker compose up --build -d service
  npm run predev
}

db=eshop

clean() {
  $cli delete-project -f $db || true
  rm -rf /tmp/cli-test/
}

scaffold() {
  $cli local up

  clean

  $cli create project $db \
    --schema-template=ecommerce \
    --framework="$2" \
    --language "$1" \
    --package-name="$3" \
    --components="$4" \
    --output-directory=/tmp/cli-test

  $cli local down
}

test_gin_go() {
  scaffold go gin "github.com/tigrisdata/$db"

  tree /tmp/cli-test/$db
  cd /tmp/cli-test/$db

  task run:docker

  test_crud_routes

  task clean

  cd -

  clean
}

test_express_typescript() {
  scaffold typescript express "eshop"

  tree /tmp/cli-test/$db
  cd /tmp/cli-test/$db

  npm i

  export PORT=3000

  start_service

  cd -

  test_crud_routes

  docker compose down

  clean
}

test_nextjs_typescript() {
  scaffold typescript nextjs "eshop"

  tree /tmp/cli-test/$db
  cd /tmp/cli-test/$db

  npm i

  export PORT=3000
  export APP_ENV=development

  start_service

  cd -

  test_crud_routes "api/"

  docker compose down

  clean
}

test_spring_java() {
  $cli local up

  clean

  scaffold java spring "com.tigrisdata.$db"

  $cli local down

  tree /tmp/cli-test/$db
  cd /tmp/cli-test/$db

  export PORT=8080

  start_service

  test_crud_routes

  docker compose down

  cd -

  clean
}

test_scaffold() {
  #FIXME: Reenabled after next CLI release
  #The tests use released version of CLI
  #and unreleased commit adds Colima support on MacOS
  #and fixes server health check on startup
  return

  test_gin_go
  test_express_typescript
  test_spring_java
  test_nextjs_typescript
}

test_scaffold
