#!/bin/bash

set -e

if [ -n "$TIGRIS_LOCAL_DEBUG" ]; then
	set -x
fi

D=/var/lib/tigris

function http_post_json() {
	curl -s --fail --location --request POST "$1" \
		--header 'X-JWT-AUD: https://tigris-local-aud' \
		--header 'Content-Type: application/json' \
		--data-raw "$2"
}

function http_post_form() {
	curl -s --fail --location --request POST "$1" \
		--header 'X-JWT-AUD: https://tigris-local-aud' \
		--header 'Content-Type: application/x-www-form-urlencoded' \
		--data-urlencode "$2" \
		--data-urlencode "$3"
}

function start_typesense() {
	/usr/bin/typesense-server --config=$D/typesense/config/typesense-server.ini &
}

function health() {
	while ! curl -s --fail "$1/health"; do
		echo "Waiting for health at $1"
		sleep 1
	done
}

function expect_typesense() {
	while ! curl -s --fail -H "X-TYPESENSE-API-KEY: ${SEARCH_AUTH_KEY}" "localhost:8108/$1" | grep "$2"; do
		echo "Waiting for typesense $1 become $2"
		sleep 1
	done
}

function wait_for_typesense() {
	echo "Waiting for typesense to start"

	SEARCH_AUTH_KEY=$(grep "api-key = " $D/typesense/config/typesense-server.ini|sed 's/api-key = //g')

	expect_typesense status LEADER
	expect_typesense collections ""

	echo "Finished waiting Typesense to start"
}

function start_fdb() {
	fdbserver --listen-address 127.0.0.1:4500 --public-address 127.0.0.1:4500 \
		--datadir $D/foundationdb/data --logdir $D/foundationdb/logs \
		--locality-zoneid tigris --locality-machineid tigris &
}

function start_gotrue() {
	echo "Starting GoTrue"
	/usr/bin/gotrue -c $D/gotrue/config/.env 2>>$D/gotrue/logs/stderr 1>>$D/gotrue/logs/stdout &
	health http://localhost:8086
	echo "Started GoTrue"
}

function start_server() {
	/server/service -c $D/server/config/server.yaml 2>>$D/server/logs/stderr 1>>$D/server/logs/stdout &
}

function signup_user() {
	http_post_json localhost:8086/signup '{
		"email": "'"$1"'",
		"password": "'"$2"'",
		"app_data": {
			"tigris_namespace": "'"$3"'"
		}
	}' 2>/dev/null 1>/dev/null
}

# Create symmetric token to auth GoTrue on Tigris
# Shared secret GOTRUE_JWT_SECRET is used to validate this token by Tigris server
function get_hs256_token() {
	# temporary server instance is started on 9081
	GOTRUE_DB_URL=http://localhost:9081 \
	GOTRUE_JWT_ALGORITHM=HS256 GOTRUE_JWT_EXP=315360000 /usr/bin/gotrue 2>/dev/null 1>/dev/null &
	gpid=$!

	health http://localhost:8086

	k=$(gen_key)

	signup_user "tigris_gotrue@m2m.tigrisdata.com" "$k" tigris_gotrue

	server_admin_token=$(http_post_form 'localhost:8086/token?grant_type=password' \
		'username=tigris_gotrue@m2m.tigrisdata.com' \
		'password='"$k"''| jq -r .access_token)

	kill $gpid
	wait $gpid
}

# Create user admin token which will be propagated to the host
# to authenticate local instance administrator
function get_user_admin_token() {
	GOTRUE_DB_URL=http://localhost:9081 \
	GOTRUE_JWT_ALGORITHM=RS256 GOTRUE_JWT_EXP=315360000 /usr/bin/gotrue 2>/dev/null 1>/dev/null &
	gpid=$!

	health http://localhost:8086

	k=$(gen_key)

	signup_user "user_admin@m2m.tigrisdata.com" "$k" default_namespace

	user_admin_token=$(http_post_form 'localhost:8086/token?grant_type=password' \
		'username=user_admin@m2m.tigrisdata.com' \
		'password='"$k"''| jq -r .access_token)

	echo "$user_admin_token" >$D/user_admin_token.txt

	kill $gpid
	wait $gpid
}

function create_admin_namespaces() {
	http_post_json localhost:9081/v1/management/namespaces/create \
		'{ "name": "tigris_gotrue", "id": "tigris_gotrue" }'

#	http_post_json localhost:9081/v1/management/namespaces/create \
#		'{ "name": "default_namespace", "id": "default_namespace" }'
}

function create_tokens_and_namespaces() {
	# start on different port so it's not visible outside docker
	TIGRIS_SERVER_SERVER_PORT=9081 start_server # start unauthenticated server instance
	spid=$!

	get_hs256_token

	if [ -n "$TIGRIS_LOCAL_GENERATE_ADMIN_TOKEN" ]; then
    get_user_admin_token
	fi

	create_admin_namespaces

	kill $spid
	wait $spid
}

# generate random password
function gen_key() {
	head -c 256 /dev/urandom | md5sum | cut -f 1 -d ' '
}

function bootstrap_auth_post() {
	echo -e "\nBootstrapping auth post"

	if [ -z "$INITIALIZING" ]; then
		echo "Already initialized"
		return
	fi

	# Create super user to access GoTrue from Tigris auth provider
	# shellcheck disable=SC2153
	/usr/bin/gotrue admin --instance_id="00000000-0000-0000-0000-000000000000" --aud="$GOTRUE_SUPERADMIN_AUD" --superadmin=true createuser "$GOTRUE_SUPERADMIN_USERNAME" "$GOTRUE_SUPERADMIN_PASSWORD" 2>/dev/null 1>/dev/null

	echo "Finished bootstrapping auth post"
}

function bootstrap_auth_pre() {
	echo -e "\nBootstrapping auth"

	if [ -z "$INITIALIZING" ]; then
		echo "Already initialized"
		return
	fi

	ssh-keygen -m PEM -t rsa -b 4096 -f $D/gotrue/config/key -N "" -C "localhost:8086" >/dev/null
	ssh-keygen -e -m pkcs8 -f $D/gotrue/config/key.pub >$D/gotrue/config/key_pem.pub

	gotrue_secret=$(gen_key)
	gotrue_jwt_secret=$(gen_key)
	gotrue_superadmin_password=$(gen_key)
	gotrue_db_encryption_key=$(gen_key)

	cat <<EOF >$D/gotrue/config/.env
GOTRUE_SECRET=$gotrue_secret
PORT=8086
GOTRUE_MAILER_AUTOCONFIRM=true
GOTRUE_DB_PROJECT=tigris_gotrue
GOTRUE_DB_URL=http://localhost:8081
GOTRUE_SITE_URL=http://localhost:8086
GOTRUE_JWT_ALGORITHM=RS256
GOTRUE_JWT_RSA_PRIVATE_KEY=$D/gotrue/config/key
GOTRUE_JWT_RSA_PUBLIC_KEYS=$D/gotrue/config/key_pem.pub
GOTRUE_DISABLE_SIGNUP=false
GOTRUE_JWT_EXP=300
GOTRUE_JWT_ISSUER=http://localhost:8086
GOTRUE_DB_BRANCH=main
GOTRUE_DB_ENCRYPTION_KEY=$gotrue_db_encryption_key
GOTRUE_OPERATOR_TOKEN=unused_token
GOTRUE_JWT_SECRET=$gotrue_jwt_secret
JWT_DEFAULT_GROUP_NAME=user
CREATE_SUPER_ADMIN_USER=true
GOTRUE_INSTANCE_ID=00000000-0000-0000-0000-000000000000
GOTRUE_SUPERADMIN_AUD=https://tigris-local-aud
GOTRUE_SUPERADMIN_USERNAME=tigris_server@m2m.tigrisdata.com
GOTRUE_SUPERADMIN_PASSWORD=$gotrue_superadmin_password
GOTRUE_LOG_FORMAT=json
TIGRIS_SKIP_LOCAL_TLS=1
EOF

	lvl="error"
	if [ -n "$TIGRIS_LOCAL_DEBUG" ]; then
		lvl="debug"
	fi

	cat <<EOF >>$D/gotrue/config/.env
GOTRUE_LOG_LEVEL=$lvl
EOF

	set -o allexport
	# shellcheck disable=SC1091,SC1090
	source $D/gotrue/config/.env
	set +o allexport

	create_tokens_and_namespaces

	cat <<EOF >>$D/gotrue/config/.env
GOTRUE_DB_TOKEN=$server_admin_token

EOF

	export GOTRUE_DB_TOKEN=$server_admin_token

	cat <<EOF >>$D/server/config/server.yaml
auth:
  enabled: true
  authz:
    enabled: true
    log_only: false
  enable_namespace_isolation: true
  enable_oauth: true
  oauth_provider: gotrue
  log_only: false
  admin_namespaces:
    - default_namespace
    - tigris_gotrue
  validators:
    - issuer: http://localhost:8086
      algorithm: RS256
      audience: https://tigris-local-aud
    - issuer: http://localhost:8086
      algorithm: HS256
      audience: https://tigris-local-aud
  token_cache_size: 100
  primary_audience: https://tigris-local-aud
  gotrue:
    username_suffix: "@m2m.tigrisdata.com"
    url: http://localhost:8086
    admin_username: tigris_server@m2m.tigrisdata.com
    admin_password: $gotrue_superadmin_password
    shared_secret: $gotrue_jwt_secret
EOF

	echo "Finished bootstrapping auth"
}

# this starts bootstrapping process only if the destination directory is empty
function bootstrap_pre() {
	echo "Bootstraping the instance (pre)"

	if [ -z "$INITIALIZING" ]; then
		echo "Already initialized"
		return
	fi

	mkdir -p $D/foundationdb/{logs,data}
	mkdir -p $D/typesense/{config,logs,data}
	mkdir -p $D/gotrue/{config,logs,data}
	mkdir -p $D/server/{config,logs,data}

	SEARCH_AUTH_KEY=$(gen_key)

	cat <<EOF >$D/typesense/config/typesense-server.ini
[server]

api-address = 0.0.0.0
api-port = 8108
data-dir = $D/typesense/data
api-key = $SEARCH_AUTH_KEY
log-dir = $D/typesense/logs
EOF

	cat <<EOF >$D/server/config/server.yaml
server:
  port: 8081
  type: database
  unix_socket: /var/lib/tigris/server/unix.sock
environment: production
search:
  auth_key: $SEARCH_AUTH_KEY
  host: localhost
  chunking: true
  compression: true
kv:
  chunking: true
  compression: true
  min_compression_threshold: 1
log:
  level: debug
secondary_index:
  write_enabled: true
  read_enabled: true
  mutate_enabled: true
tracing:
  enabled: false
metrics:
  enabled: false
EOF

	echo "Finished bootstrapping the instance (pre)"
}

# continues bootstrapping process after starting FDB and Typesense
function bootstrap_post() {
	echo "Bootstraping the instance (post)"

	if [ -z "$INITIALIZING" ]; then
		echo "Already initialized"
		return
	fi

	if [ -n "$TIGRIS_LOCAL_PERSISTENCE" ]; then
		fdbcli --exec 'configure new single ssd'
	else
		fdbcli --exec 'configure new single memory'
	fi

	date > /var/lib/tigris/initialized

	echo "Finished bootstraping the instance (post)"
}

function wait_for_fdb() {
	echo "Waiting for foundationdb to start"

	OUTPUT="something_else"
	while [ "x${OUTPUT}" != "xThe database is available." ]
	do
		OUTPUT=$(fdbcli --exec 'status minimal')
		sleep 1
	done

	echo "Finished waiting for foundationdb to start"
}

main() {
	date
	echo "Starting Tigris instance"

	# shellcheck disable=SC2010,SC2143
	if [ -n "$(ls -A $D|grep -v init.log)" ]; then
		echo "Already initialized"
		unset INITIALIZING
	else
		INITIALIZING=1
	fi

	bootstrap_pre

	start_fdb
	start_typesense

	bootstrap_post

	wait_for_fdb
	wait_for_typesense

	if [ -n "$TIGRIS_BOOTSTRAP_LOCAL_AUTH" ]; then
		bootstrap_auth_pre
	fi

	if [ -n "$TIGRIS_SKIP_LOCAL_AUTH" ]; then
		export TIGRIS_SERVER_AUTH_ENABLED=false
		export TIGRIS_SERVER_AUTH_ENABLE_NAMESPACE_ISOLATION=false
		echo "Starting with authentication disabled"
	fi

	start_server
	pid=$!

	health "http://localhost:8081/v1"

	if [ -n "$TIGRIS_BOOTSTRAP_LOCAL_AUTH" ]; then
		bootstrap_auth_post
	fi

	if [ -f $D/gotrue/config/.env ] && [ -z "$TIGRIS_SKIP_LOCAL_AUTH" ] ; then
		start_gotrue
	fi

	echo "Successfully started"
	date

	wait $pid
}

main 2>&1 | tee -a $D/init.log

