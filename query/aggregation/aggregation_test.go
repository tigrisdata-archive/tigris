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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisdb/query/expression"
)

func TestAggregation(t *testing.T) {
	e, err := expression.Unmarshal([]byte(`{"$sum": "$qty"}`), UnmarshalAggObject)
	require.NoError(t, err)
	require.Equal(t, e.(Aggregation).(*AccumulatorOp).Type, "$sum")

	e, err = expression.Unmarshal([]byte(`{ "$sum": [ "$final", "$midterm" ] }`), UnmarshalAggObject)
	require.NoError(t, err)
	require.Equal(t, e.(Aggregation).(*AccumulatorOp).Type, "$sum")

	e, err = expression.Unmarshal([]byte(`{ "$avg": {"$multiply": [ "$final", "$midterm" ]}}`), UnmarshalAggObject)
	require.NoError(t, err)
	require.Equal(t, e.(Aggregation).(*AccumulatorOp).Type, "$avg")
	require.Equal(t, e.(Aggregation).(*AccumulatorOp).Agg.(*ArithmeticOp).Type, "$multiply")
}
