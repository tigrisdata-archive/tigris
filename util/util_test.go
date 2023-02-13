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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLastItem(t *testing.T) {
	nums := []int{1, 2, 3}
	assert.Equal(t, 3, Last(nums))

	type simple struct {
		val string
	}

	structs := []simple{{val: "1"}, {val: "2"}}
	assert.Equal(t, "2", Last(structs).val)

	empty := []string{}
	assert.Equal(t, "", Last(empty))
}
