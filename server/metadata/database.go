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
	BranchNamePrefix    = "$branch$_"
	BranchNameSeparator = "_"
)

type DatabaseBranch struct {
	db     string
	branch string
}

func NewDatabaseBranch(db string, branch string) *DatabaseBranch {
	if branch == MainBranch {
		branch = ""
	}
	return &DatabaseBranch{
		db:     db,
		branch: branch,
	}
}

func NewBranchFromDbName(key string) *DatabaseBranch {
	if !strings.HasPrefix(key, BranchNamePrefix) {
		return NewDatabaseBranch(key, "")
	}
	s := strings.SplitN(key, BranchNameSeparator, 3)
	return NewDatabaseBranch(s[1], s[2])
}

func (b *DatabaseBranch) Name() string {
	if b.IsMain() {
		return b.Db()
	}
	return BranchNamePrefix + b.Db() + BranchNameSeparator + b.Branch()
}

func (b *DatabaseBranch) Db() string {
	return b.db
}

func (b *DatabaseBranch) Branch() string {
	if b.IsMain() {
		return MainBranch
	}
	return b.branch
}

func (b *DatabaseBranch) IsMain() bool {
	if len(b.branch) == 0 || b.branch == MainBranch {
		return true
	}
	return false
}
