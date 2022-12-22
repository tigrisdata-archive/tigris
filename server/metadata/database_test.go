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

package metadata

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestDatabaseName(t *testing.T) {
	t.Run("with empty branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "main")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseNameWithBranch("myDb", "staging")

		assert.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "staging", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMainBranch())
	})
}

func TestBranchFromDbName(t *testing.T) {
	t.Run("with db name only", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with main branch name", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_main")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with some other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$_staging")

		assert.Equal(t, "myDb_$branch$_staging", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "staging", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMainBranch())
	})

	t.Run("with empty string", func(t *testing.T) {
		databaseBranch := NewDatabaseName("")

		assert.Equal(t, "", databaseBranch.Name())
		assert.Equal(t, "", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMainBranch())
	})

	t.Run("with multiple separators", func(t *testing.T) {
		databaseBranch := NewDatabaseName("myDb_$branch$__prod_staging_2")

		assert.Equal(t, "myDb_$branch$__prod_staging_2", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "_prod_staging_2", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMainBranch())
	})
}
