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

package metrics

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/assert"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

func TestTagsHelpers(t *testing.T) {
	t.Run("Test mergeTags", func(t *testing.T) {
		tagSet1 := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key4": "unknown",
		}
		tagSet2 := map[string]string{
			"key1": "value1",
			"key3": "value3",
		}
		tagSet3 := map[string]string{
			"key4": "value4",
			"key5": "value5",
		}
		mergedTagSet := mergeTags(tagSet1, tagSet2, tagSet3)
		assert.Equal(t, "value1", mergedTagSet["key1"])
		assert.Equal(t, "value2", mergedTagSet["key2"])
		assert.Equal(t, "value3", mergedTagSet["key3"])
		assert.Equal(t, "value4", mergedTagSet["key4"])
		assert.Equal(t, "value5", mergedTagSet["key5"])
	})

	t.Run("Test getTagsForError", func(t *testing.T) {
		assert.Equal(t, map[string]string{
			"error_source": "test_source",
			"error_value":  "none",
		}, getTagsForError(nil, "test_source"))

		// For specific errors, the source is ignored
		fdbErrTags := getTagsForError(fdb.Error{Code: 1}, "ignored_source")
		assert.Equal(t, "fdb", fdbErrTags["error_source"])
		assert.Equal(t, "1", fdbErrTags["error_value"])

		// For specific errors, the source is ignored
		tigrisErrTags := getTagsForError(&api.TigrisError{Code: api.Code_NOT_FOUND}, "ignored_source")
		assert.Equal(t, "tigris_server", tigrisErrTags["error_source"])
		assert.Equal(t, "NOT_FOUND", tigrisErrTags["error_value"])
	})

	t.Run("Test getDbTags", func(t *testing.T) {
		assert.Equal(t, map[string]string{"db": "foobar"}, GetDbCollTags("foobar", ""))
	})

	t.Run("Test getDbCollTags", func(t *testing.T) {
		dbCollTags := GetDbCollTags("foodb", "foocoll")
		assert.Equal(t, "foodb", dbCollTags["db"])
		assert.Equal(t, "foocoll", dbCollTags["collection"])
	})
}
