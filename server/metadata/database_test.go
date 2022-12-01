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
		databaseBranch := NewBranchFromDbName("myDb")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMain())
	})

	t.Run("with main branch", func(t *testing.T) {
		databaseBranch := NewDatabaseBranch("myDb", "main")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMain())
	})

	t.Run("with other branch", func(t *testing.T) {
		databaseBranch := NewDatabaseBranch("myDb", "staging")

		assert.Equal(t, "$branch$_myDb_staging", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "staging", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMain())
	})
}

func TestBranchFromDbName(t *testing.T) {
	t.Run("with db name only", func(t *testing.T) {
		databaseBranch := NewBranchFromDbName("myDb")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMain())
	})

	t.Run("with main branch name", func(t *testing.T) {
		databaseBranch := NewBranchFromDbName("$branch$_myDb_main")

		assert.Equal(t, "myDb", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMain())
	})

	t.Run("with some other branch", func(t *testing.T) {
		databaseBranch := NewBranchFromDbName("$branch$_myDb_staging")

		assert.Equal(t, "$branch$_myDb_staging", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "staging", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMain())
	})

	t.Run("with empty string", func(t *testing.T) {
		databaseBranch := NewBranchFromDbName("")

		assert.Equal(t, "", databaseBranch.Name())
		assert.Equal(t, "", databaseBranch.Db())
		assert.Equal(t, MainBranch, databaseBranch.Branch())
		assert.True(t, databaseBranch.IsMain())
	})

	t.Run("with multiple separators", func(t *testing.T) {
		databaseBranch := NewBranchFromDbName("$branch$_myDb_prod_staging_2")

		assert.Equal(t, "$branch$_myDb_prod_staging_2", databaseBranch.Name())
		assert.Equal(t, "myDb", databaseBranch.Db())
		assert.Equal(t, "prod_staging_2", databaseBranch.Branch())
		assert.False(t, databaseBranch.IsMain())
	})
}
