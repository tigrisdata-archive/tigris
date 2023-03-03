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

package search

import "github.com/tigrisdata/tigris/errors"

type Group struct {
	Hits []*Hit
	Keys []string
}

func NewGroup(keys []string, hits []*Hit) *Group {
	return &Group{
		Keys: keys,
		Hits: hits,
	}
}

type Groups struct {
	index  int
	groups []*Group
}

func NewGroups() *Groups {
	return &Groups{}
}

func (g *Groups) add(group *Group) {
	g.groups = append(g.groups, group)
}

func (g *Groups) Next() (*Group, error) {
	if g.index < len(g.groups) {
		group := g.groups[g.index]
		g.index++
		return group, nil
	}

	return nil, errors.Internal("no more hits to iterate")
}

func (g *Groups) Len() int {
	return len(g.groups)
}

func (g *Groups) HasMoreGroups() bool {
	return g.index < len(g.groups)
}
