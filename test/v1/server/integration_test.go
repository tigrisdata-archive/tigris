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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"gopkg.in/gavv/httpexpect.v1"
)

func SetupSuites() []suite.TestingSuite {
	var suites []suite.TestingSuite
	suites = append(suites, &CollectionSuite{
		database: fmt.Sprintf("integration_db1_%x", rand.Uint64()),
	})

	suites = append(suites, &DocumentSuite{
		database:   fmt.Sprintf("integration_db2_%x", rand.Uint64()),
		collection: "test_collection",
	})

	return suites
}

// TestServer will run all the test present in the suites.
// To run an individual tests use go test -tags=integration -run TestServer/TestUpdate_SingleRow ./test/v1/server/...
func TestServer(t *testing.T) {
	suites := SetupSuites()
	for _, s := range suites {
		suite.Run(t, s)
	}
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	os.Exit(m.Run())
}

func testError(resp *httpexpect.Response, status int, code api.Code, message string) {
	resp.Status(status).
		JSON().Path("$.error").Object().
		ValueEqual("message", message).ValueEqual("code", api.CodeToString(code))
}
