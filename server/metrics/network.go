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

package metrics

import (
	"github.com/uber-go/tally"
)

var (
	BytesReceived tally.Scope
	BytesSent     tally.Scope
)

func getNetworkTagKeys() []string {
	return []string{
		"grpc_method",
		"tigris_tenant",
		"env",
		"db",
		"collection",
	}
}

func initializeNetworkScopes() {
	BytesReceived = NetworkMetrics.SubScope("bytes")
	BytesSent = NetworkMetrics.SubScope("bytes")
}

func (m *Measurement) CountSentBytes(scope tally.Scope, tags map[string]string, size int) {
	if scope != nil {
		// proto.Size has int, need to convert it here
		scope.Tagged(tags).Counter("sent").Inc(int64(size))
	}
}

func (m *Measurement) CountReceivedBytes(scope tally.Scope, tags map[string]string, size int) {
	if scope != nil {
		// proto.Size has int, need to convert it here
		scope.Tagged(tags).Counter("received").Inc(int64(size))
	}
}
