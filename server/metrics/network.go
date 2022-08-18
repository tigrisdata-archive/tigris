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

import "github.com/uber-go/tally"

var (
	BytesReceived tally.Scope
	BytesSent     tally.Scope
)

func initializeNetorkScopes() {
	BytesReceived = NetworkMetrics.SubScope("bytes")
	BytesSent = NetworkMetrics.SubScope("bytes")
}

func (s *SpanMeta) CountSentBytes(scope tally.Scope, size int) {
	// proto.Size has int, need to convert it here
	scope.Tagged(s.GetTags()).Counter("sent").Inc(int64(size))
}

func (s *SpanMeta) CountReceivedBytes(scope tally.Scope, size int) {
	// proto.Size has int, need to convert it here
	scope.Tagged(s.GetTags()).Counter("received").Inc(int64(size))
}
