#!/usr/bin/env bash
# Copyright 2022-2023 Tigris Data, Inc.
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


if [ -n "${GRAFANA_URL}" ];
then
	echo "Not setting a password because an external grafana URL was specified."
fi

if [ -n "${GRAFANA_PASSWORD}" ]; then
	# If no environment variable is set, the default password will be used
	docker exec tigris_grafana grafana-cli admin reset-admin-password "${GRAFANA_PASSWORD}"
fi
