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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatadogQueryValidation(t *testing.T) {
	require.True(t, isAllowedMetricQueryInput("users"))
	require.True(t, isAllowedMetricQueryInput("user_db"))
	require.True(t, isAllowedMetricQueryInput("user_db_1"))
	require.True(t, isAllowedMetricQueryInput("requests_count_ok.count"))
	require.False(t, isAllowedMetricQueryInput("users:"))
	require.False(t, isAllowedMetricQueryInput("users "))
	require.False(t, isAllowedMetricQueryInput("users,foo:bar"))
}
