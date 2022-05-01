package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type ServerSuite struct {
	suite.Suite
}

func (s *ServerSuite) TestInfo() {
	s.Run("info_handler", func() {
		resp := info(s.T())

		resp.Status(http.StatusOK).
			JSON().
			Object().
			Value("server_version").NotNull()
	})
}

func info(t *testing.T) *httpexpect.Response {
	e := httpexpect.New(t, config.GetBaseURL())
	return e.GET(config.GetBaseURL() + "/info").
		Expect()
}
