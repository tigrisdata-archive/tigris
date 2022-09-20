#!/usr/bin/env bash

if [ -n "${GRAFANA_URL}" ];
then
	echo "Not setting a password because an external grafana URL was specified."
fi

if [ -n "${GRAFANA_PASSWORD}" ]; then
	# If no environment variable is set, the default password will be used
	docker exec tigris_grafana grafana-cli admin reset-admin-password "${GRAFANA_PASSWORD}"
fi
