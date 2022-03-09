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

package read

import (
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/grpc/codes"
)

func TestBuildFields(t *testing.T) {
	f, err := BuildFields([]byte(`{"a": 1, "b": true}`))
	require.NoError(t, err)
	require.Equal(t, f.Include["a"].Alias(), "a")
	require.True(t, f.Include["a"].Include())

	require.Equal(t, f.Include["b"].Alias(), "b")
	require.True(t, f.Include["b"].Include())
	require.True(t, len(f.Exclude) == 0)

	f, err = BuildFields([]byte(`{"a": 0}`))
	require.NoError(t, err)
	require.False(t, f.Exclude["a"].Include())
	require.True(t, len(f.Include) == 0)

	f, err = BuildFields([]byte(`{"b": false}`))
	require.NoError(t, err)
	require.False(t, f.Exclude["b"].Include())

	f, err = BuildFields([]byte(`{"a": 1, "b": true, "c": {"$avg": "$f1"}, "d": {"$sum": ["$f2", "$f3"]}}`))
	require.Error(t, api.Errorf(codes.InvalidArgument, "only boolean/integer is supported as value"), err)
	require.Nil(t, f)
}
