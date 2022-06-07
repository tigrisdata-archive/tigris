#!/usr/bin/env bash

if [ "x${GRAFANA_URL}" != "x" ];
then
	echo "Not setting a password because an external grafana URL was specified."
fi

if [ "x${GRAFANA_PASSWORD}" != "x" ]; then
	# If no environment variable is set, the default password will be used
	docker exec tigris_grafana grafana-cli admin reset-admin-password ${GRAFANA_PASSWORD}
fi
