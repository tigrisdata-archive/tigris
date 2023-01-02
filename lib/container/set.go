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

package container

type HashSet struct {
	stringMap map[string]struct{}
}

func NewHashSet(s ...string) HashSet {
	set := HashSet{
		stringMap: make(map[string]struct{}, len(s)*2),
	}
	for _, ss := range s {
		set.Insert(ss)
	}
	return set
}

func (set *HashSet) Length() int {
	return len(set.stringMap)
}

func (set *HashSet) Insert(s ...string) {
	for _, ss := range s {
		set.stringMap[ss] = struct{}{}
	}
}

func (set *HashSet) Contains(s string) bool {
	if _, ok := set.stringMap[s]; ok {
		return true
	}
	return false
}

func (set *HashSet) ToList() []string {
	list := make([]string, 0, len(set.stringMap))
	for k := range set.stringMap {
		list = append(list, k)
	}
	return list
}
