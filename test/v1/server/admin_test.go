package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type AdminTestMap map[string]interface{}

type AdminSuite struct {
	suite.Suite
}

func adminExpectLow(s httpexpect.LoggerReporter, url string) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  url,
		Reporter: httpexpect.NewAssertReporter(s),
	})
}

func adminExpect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return adminExpectLow(s, config.GetBaseURL())
}

func (s *AdminSuite) TestCreateNamespace() {
	resp := createNamespace(s.T(), "namespace-a", 1)
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", "database created successfully")
}

func createNamespace(t *testing.T, namespaceName string, namespaceId int32) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(getCreateNamespaceURL(namespaceName)).
		WithJSON(AdminTestMap{"id": namespaceId}).
		Expect()
}

func getCreateNamespaceURL(namespaceName string) string {
	return fmt.Sprintf("/admin/v1/namespaces/%s/create", namespaceName)
}
