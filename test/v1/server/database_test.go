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
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	"gopkg.in/gavv/httpexpect.v1"
)

type DatabaseSuite struct {
	suite.Suite
}

func getDatabaseURL(databaseName string, methodName string) string {
	return fmt.Sprintf("/api/v1/databases/%s/%s", databaseName, methodName)
}

func (s *DatabaseSuite) TestCreateDatabase() {
	resp := createDatabase(s.T(), "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "database created successfully")
}

func (s *DatabaseSuite) TestDescribeDatabase() {
	resp := describeDatabase(s.T(), "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("db", "test_db")
}

func (s *DatabaseSuite) TestDropDatabase_NotFound() {
	resp := dropDatabase(s.T(), "test_drop_db_not_found")
	resp.Status(http.StatusNotFound).
		JSON().
		Object().
		ValueEqual("message", "database doesn't exist 'test_drop_db_not_found'")
}

func (s *DatabaseSuite) TestDropDatabase() {
	resp := dropDatabase(s.T(), "test_db")
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "database dropped successfully")
}

func createDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "create")).
		Expect()
}

func dropDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.DELETE(getDatabaseURL(databaseName, "drop")).
		Expect()
}

func describeDatabase(t *testing.T, databaseName string) *httpexpect.Response {
	e := expect(t)
	return e.POST(getDatabaseURL(databaseName, "describe")).Expect()
}
