#!/usr/bin/env bash
# Copyright 2022 Tigris Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [ -z "${GRAFANA_PASSWORD}" ]; then
	# default password is used
	GRAFANA_PASSWORD="admin"
fi

if [ -z "${GRAFANA_URL}" ]; then
	# default grafana url is used
	GRAFANA_URL="http://localhost:3000"
fi

if [ -z "${PROMETHEUS_URL}" ]; then
	PROMETHEUS_URL="http://tigris_prometheus:9090"
fi

function add_prometheus {
	curl -s \
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
		echo "Grafana available at: $GRAFANA_URL"
		echo "Prometheus available at: http://localhost:9090"
		break
	else
		echo "Adding prometheus was not successful, retrying soon. Iteration: $i"
		sleep 0.5
	fi
done
