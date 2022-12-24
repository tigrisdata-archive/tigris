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

import "strings"

type DatabaseMetadata struct {
	Id        uint32
	Creator   string
	CreatedAt int64
}

func (dm *DatabaseMetadata) SetDatabaseId(id uint32) {
	dm.Id = id
}

const (
	MainBranch          = "main"
	BranchNameSeparator = "_$branch$_"
)

// DatabaseName represents a primary database and its branch name.
type DatabaseName struct {
	db     string
	branch string
}

func NewDatabaseNameWithBranch(db string, branch string) *DatabaseName {
	if branch == MainBranch {
		branch = ""
	}
	return &DatabaseName{
		db:     db,
		branch: branch,
	}
}

func NewDatabaseName(key string) *DatabaseName {
	s := strings.Split(key, BranchNameSeparator)
	if len(s) < 2 {
		return NewDatabaseNameWithBranch(s[0], "")
	}
	return NewDatabaseNameWithBranch(s[0], s[1])
}

// Name returns the internal name of the database
// db with name "catalog" and branch "main" will have internal name as "catalog"
// db with name "catalog" and branch "feature" will have internal name as "catalog_$branch$_feature".
func (b *DatabaseName) Name() string {
	if b.IsMainBranch() {
		return b.Db()
	}
	return b.Db() + BranchNameSeparator + b.Branch()
}

// Db is the user facing name of the oprimary database.
func (b *DatabaseName) Db() string {
	return b.db
}

// Branch belonging to Db.
func (b *DatabaseName) Branch() string {
	if b.IsMainBranch() {
		return MainBranch
	}
	return b.branch
}

// IsMainBranch returns "True" if this is primary Db or "False" if a branch.
func (b *DatabaseName) IsMainBranch() bool {
	if len(b.branch) == 0 || b.branch == MainBranch {
		return true
	}
	return false
}
