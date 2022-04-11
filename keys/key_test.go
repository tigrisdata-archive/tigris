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

package keys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	k := NewKey(nil, int64(10))
	require.Nil(t, k.Table())
	require.Equal(t, []interface{}{int64(10)}, k.IndexParts())

	k = NewKey([]byte("foo"), []interface{}{int64(5)}...)
	require.Equal(t, []interface{}{int64(5)}, k.IndexParts())
	require.Equal(t, []byte("foo"), k.Table())
}
