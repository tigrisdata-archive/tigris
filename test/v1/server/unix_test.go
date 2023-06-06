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

//go:build (linux || darwin) && integration

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	config2 "github.com/tigrisdata/tigris/test/config"
	"github.com/tigrisdata/tigris/util"
	"gopkg.in/gavv/httpexpect.v1"
)

func getUnixHTTPDialer(url string) func(_ context.Context, _, _ string) (net.Conn, error) {
	var dialer func(_ context.Context, _, _ string) (net.Conn, error)

	if url[0] == '/' || url[0] == '.' {
		dialer = func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", url)
		}
	}

	return dialer
}

func TestUnixSocket(t *testing.T) {
	t.Skip("socket only available when started using `make local_run`")

	db := fmt.Sprintf("integration_%s", t.Name())
	deleteProject(t, db)
	createProject(t, db).Status(http.StatusOK)

	sock := "/tmp/tigris_test_server.sock"

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: getUnixHTTPDialer(sock),
		},
	}

	e := httpexpect.WithConfig(httpexpect.Config{
		Reporter: httpexpect.NewRequireReporter(t),
		Client:   client,
		BaseURL:  config2.GetBaseURL(),
	})

	e.DELETE(getCollectionURL(db, testCollection, "drop")).
		Expect()

	e.POST(getCollectionURL(db, testCollection, "createOrUpdate")).
		WithJSON(testCreateSchema).
		Expect().Status(http.StatusOK)
}

func TestReadPeerCreds(t *testing.T) {
	sock := "/tmp/tigris_creds_test.sock"
	_ = os.Remove(sock)

	l, err := net.Listen("unix", sock)
	require.NoError(t, err)

	_, err = net.Dial("unix", sock)
	require.NoError(t, err)

	conn, err := l.Accept()
	require.NoError(t, err)

	uid, err := util.ReadPeerCreds(conn)
	require.NoError(t, err)

	require.Equal(t, os.Geteuid(), int(uid))
}
