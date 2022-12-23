// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/tigrisdata/tigris/server/config"
)

func GetBaseURL() string {
	config.LoadEnvironment()

	if config.GetEnvironment() == config.EnvTest {
		return "http://tigris_server:8081"
	}
	return "http://localhost:8081"
}

func GetBaseRealtimeURL() string {
	config.LoadEnvironment()

	if config.GetEnvironment() == config.EnvTest {
		return "http://tigris_realtime:8083"
	}
	return "http://localhost:8083"
}

func GetBaseURL2() string {
	config.LoadEnvironment()

	if config.GetEnvironment() == config.EnvTest {
		return "http://tigris_server2:8081"
	}
	return "http://localhost:8082"
}
