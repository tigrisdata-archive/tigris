#!/bin/bash

set -ex

TMP_DIR=/tmp/cli-test
PORT=8080

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

# first parameter is to add /api to the path for nextjs app
# second parameter is to pluralize read path for express and gin
test_crud_routes() {
  OS=$(uname -s)
  if [ "$OS" == "Darwin" ]; then
    # MacOS is slow in Github actions
    sleep 25 # give some time server to start
  else
    sleep 7 # give some time server to start
  fi

  request "$1"users"$4" '{"id":1, "name":"John","balance":100}' #Id=1
  request "$1"users"$4" '{"id":2, "name":"Jane","balance":200}' #Id=2

  request "$1"products"$4" '{"id":1, "name":"Avocado","price":10,"quantity":5}' #Id=1
  request "$1"products"$4" '{"id":2, "name":"Gold","price":3000,"quantity":1}' #Id=2

  #low balance
  request "$1"orders"$4" '{"id":1, "user_id":1,"productItems":[{"Id":2,"Quantity":1}]}' || true
  # low stock
  request "$1"orders"$4" '{"id":2, "user_id":1,"productItems":[{"Id":1,"Quantity":10}]}' || true

  request "$1"orders"$4" '{"id":3, "user_id":1,"productItems":[{"Id":1,"Quantity":5}]}' #Id=1

  curl --fail localhost:$PORT/"$1"user"$2"/1
  echo
  curl --fail localhost:$PORT/"$1"product"$2"/1
  echo
  curl --fail localhost:$PORT/"$1"order"$2"/1
  echo

  # search
  if [ "$3" == "search is post" ]; then
    request "$1"users/search '{"q":"john"}'
    request "$1"products/search '{"q":"avocado","searchFields": ["name"]}'
  elif [ -z "$3" ]; then
    curl --fail "localhost:$PORT/${1}users/search?q=john"
    curl --fail "localhost:$PORT/${1}products/search?q=avocado&searchFields=name"
  fi
}

start_service() {
  TIGRIS_URL=tigris-local-server:8081 docker compose up -d tigris
  TIGRIS_LOG_LEVEL=debug $cli ping --timeout=40s
  $cli create project "$db"
  TIGRIS_URL=tigris-local-server:8081 docker compose up --build -d service
}

db=eshop

clean() {
  $cli delete-project -f $db || true
  rm -rf /tmp/cli-test/$db
}

scaffold() {
	$cli local up 8081

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

  export PORT=8080

  test_crud_routes "" "s"

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

  test_crud_routes "" "s" "search is post"

  docker compose down

  cd -

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
  npm run predev

  test_crud_routes "api/"

  docker compose down

  cd -

  clean
}

test_spring_java() {
  clean

  scaffold java spring "com.tigrisdata.$db"

  $cli local down

  tree /tmp/cli-test/$db
  cd /tmp/cli-test/$db

  sed -i'' -e "s/localhost:8081/tigris-local-server:8081/" src/main/resources/application.yml

  export PORT=8080

  start_service

  test_crud_routes "" "s" "no search" "/"

  docker compose down

  cd -

  clean
}

test_scaffold() {
  test_gin_go
  test_express_typescript
  test_spring_java
  test_nextjs_typescript

  # Bring local instance back in case it was stopped by scaffold tests
  if [ -z "$noup" ]; then
	  TIGRIS_LOG_LEVEL=debug $cli local up 8081
  fi
}
