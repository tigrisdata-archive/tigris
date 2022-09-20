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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/value"
)

func TestExpression(t *testing.T) {
	expr, err := Unmarshal([]byte(`["$foo", "bar", 1, 1.01, false]`), nil)
	require.NoError(t, err)
	listExpr, ok := expr.([]Expr)
	require.True(t, ok)
	require.Equal(t, 5, len(listExpr))
	require.Equal(t, "$foo", listExpr[0].(value.Value).(*value.StringValue).Value)
	require.Equal(t, "bar", listExpr[1].(value.Value).(*value.StringValue).Value)
	require.Equal(t, int64(1), int64(*listExpr[2].(value.Value).(*value.IntValue)))
	require.Equal(t, 1.01, listExpr[3].(value.Value).(*value.DoubleValue).Double)
	require.Equal(t, false, bool(*listExpr[4].(value.Value).(*value.BoolValue)))
}
