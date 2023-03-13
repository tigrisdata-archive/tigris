// Copyright 2022-2023 Tigris Data, Inc.
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

package quota

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
)

func TestNodeQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ns := fmt.Sprintf("ns-test-tenantQuota-1-%x", rand.Uint64()) //nolint:gosec

	m := initNode(&config.QuotaConfig{
		Node: config.LimitsConfig{
			ReadUnits:  10,
			WriteUnits: 10,
		},
	})

	require.NoError(t, m.Allow(ctx, ns, 6000, false)) // 2 units: 4096 + something
	require.NoError(t, m.Allow(ctx, ns, 6000, true))  // 2 units: 4096 + something

	var err error

	i := 0
	for ; err != ErrReadUnitsExceeded && i < 50; i++ {
		err = m.Allow(ctx, ns, 2048, false)
	}
	assert.Equal(t, 9, i)

	i = 0
	for ; err != ErrWriteUnitsExceeded && 1 < 50; i++ {
		err = m.Allow(ctx, ns, 2048, true) // < 4096 = 1 unit
	}
	assert.Equal(t, 9, i)

	// quota is per node so any namespace should be rejected
	require.Equal(t, ErrWriteUnitsExceeded, m.Allow(ctx, ns+"_other", 1, true))
}
