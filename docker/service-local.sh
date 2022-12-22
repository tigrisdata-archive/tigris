#!/bin/bash

function start_typesense() {
	/usr/bin/typesense-server --config=/etc/typesense/typesense-server.ini &
}

function wait_for_typesense() {
	echo "Waiting for typesense to start"
	IS_LEADER=1
  while [ ${IS_LEADER} -ne 0 ]
  do
		curl -H "X-TYPESENSE-API-KEY: ${TIGRIS_SERVER_SEARCH_AUTH_KEY}" localhost:8108/status | grep LEADER
		IS_LEADER=$?
		sleep 2
	done

}

function start_fdb() {
	fdbserver --listen-address 127.0.0.1:4500 --public-address 127.0.0.1:4500 --datadir /var/lib/foundationdb/data --logdir /var/lib/foundationdb/logs --locality-zoneid tigris --locality-machineid tigris &
	fdbcli --exec 'configure new single memory'
}

function wait_for_fdb() {
	echo "Waiting for foundationdb to start"
	OUTPUT="something_else"
	while [ "x${OUTPUT}" != "xThe database is available." ]
	do
		OUTPUT=$(fdbcli --exec 'status minimal')
		sleep 2
	done
}

export TIGRIS_SERVER_TYPE=database
export TIGRIS_SERVER_SEARCH_AUTH_KEY=ts_dev_key
export TIGRIS_SERVER_SEARCH_HOST=localhost
export TIGRIS_SERVER_CDC_ENABLED=true
start_fdb
wait_for_fdb
start_typesense
wait_for_typesense
/server/service
