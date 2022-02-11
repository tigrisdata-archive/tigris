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

package filter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestValue(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		i := IntValue(5)
		r, err := i.CompareTo(NewValue(structpb.NewNumberValue(5)))
		require.NoError(t, err)
		require.Equal(t, 0, r)

		r, err = i.CompareTo(NewValue(structpb.NewNumberValue(7)))
		require.NoError(t, err)
		require.Equal(t, -1, r)

		r, err = i.CompareTo(NewValue(structpb.NewNumberValue(0)))
		require.NoError(t, err)
		require.Equal(t, 1, r)

		r, err = i.CompareTo(NewValue(structpb.NewStringValue("5")))
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("string", func(t *testing.T) {
		i := StringValue("abc")
		r, err := i.CompareTo(NewValue(structpb.NewStringValue("abc")))
		require.NoError(t, err)
		require.Equal(t, 0, r)

		r, err = i.CompareTo(NewValue(structpb.NewStringValue("ac")))
		require.NoError(t, err)
		require.Equal(t, -1, r)

		r, err = i.CompareTo(NewValue(structpb.NewStringValue("abb")))
		require.NoError(t, err)
		require.Equal(t, 1, r)

		r, err = i.CompareTo(NewValue(structpb.NewNumberValue(5)))
		require.Equal(t, fmt.Errorf("wrong type compared "), err)
		require.Equal(t, -2, r)
	})
	t.Run("bool", func(t *testing.T) {
		i := BoolValue(false)
		r, err := i.CompareTo(NewValue(structpb.NewBoolValue(false)))
		require.NoError(t, err)
		require.Equal(t, 0, r)

		r, err = i.CompareTo(NewValue(structpb.NewBoolValue(true)))
		require.NoError(t, err)
		require.Equal(t, -1, r)

		i = BoolValue(true)
		r, err = i.CompareTo(NewValue(structpb.NewBoolValue(false)))
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}
