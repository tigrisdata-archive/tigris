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

//go:build integration

package server

import (
	"testing"

	"github.com/tigrisdata/tigris/test/config"
)

func TestGoTrueAuthProvider(t *testing.T) {
	e2 := expectLow(t, config.GetBaseURL2())

	_ = e2.POST(getDocumentURL("db1", "coll1", "insert")).Expect()
}
