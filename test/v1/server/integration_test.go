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

//go:build integration

package server

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func SetupSuites() []suite.TestingSuite {
	var suites []suite.TestingSuite
	suites = append(suites, &DatabaseSuite{})

	suites = append(suites, &CollectionSuite{
		database: "db1",
	})

	suites = append(suites, &DocumentSuite{
		database:   "db2",
		collection: "c1",
	})
	return suites
}

func TestServer(t *testing.T) {
	suites := SetupSuites()
	for _, s := range suites {
		suite.Run(t, s)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
