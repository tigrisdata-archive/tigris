#!/usr/bin/env bash

if [ "x${GRAFANA_PASSWORD}" == "x" ]; then
	# default password is used
	GRAFANA_PASSWORD="admin"
fi

if [ "x${GRAFANA_URL}" == "x" ]; then
	# default grafana url is used
	GRAFANA_URL="http://localhost:3000"
fi

if [ "x${PROMETHEUS_URL}" == "x" ]; then
	PROMETHEUS_URL="http://tigris_prometheus:9090"
fi

function add_prometheus {
	curl \
		-u admin:"${GRAFANA_PASSWORD}" \
		"${GRAFANA_URL}/api/datasources" \
		-X POST \
		--data "{\"name\": \"tigris_prometheus\", \"type\": \"prometheus\", \"url\": \"${PROMETHEUS_URL}\", \"access\": \"proxy\"}" \
		--header 'content-type: application/json'
}

max_tries=20
for i in $(seq 1 ${max_tries}); do
	if add_prometheus; then
		echo
		echo "Successfully added prometheus data source"
		break
	else
		echo "Adding prometheus was not successful, retrying soon"
		sleep 0.5
	fi
done
