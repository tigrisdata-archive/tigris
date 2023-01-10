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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseName(t *testing.T) {
	t.Run("with empty branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "main")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "staging")

		require.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "staging", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})
}

func TestBranchFromDbName(t *testing.T) {
	t.Run("with db name only", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch name", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_main")

		require.Equal(t, "myDb", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with some other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_staging")

		require.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "staging", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})

	t.Run("with empty string", func(t *testing.T) {
		databaseBranch := NewDatabaseName("")

		require.Equal(t, "", databaseBranch.Name())
		require.Equal(t, "", databaseBranch.Db())
		require.Equal(t, MainBranch, databaseBranch.Branch())
		require.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with multiple separators", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$__prod_staging_2")

		require.Equal(t, "myDb_$branch$__prod_staging_2", databaseBranch.Name())
		require.Equal(t, "myDb", databaseBranch.Db())
		require.Equal(t, "_prod_staging_2", databaseBranch.Branch())
		require.False(t, databaseBranch.IsMainBranch())
	})
}
