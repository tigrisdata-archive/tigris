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

var expectedListNamespaceResponse = map[string]interface{}{
	"namespaces": []interface{}{map[string]interface{}{
		"id":   1,
		"name": "default_namespace",
	},
		map[string]interface{}{
			"id":   2,
			"name": "namespace-a",
		},
	},
}

func (s *AdminSuite) TestCreateNamespace() {
	s.Run("create_namespace", func() {
		resp := createNamespace(s.T(), "namespace-a", 2)
		resp.Status(http.StatusOK).
			JSON().
			Object().
			ValueEqual("message", "Namespace created, with id=2, and name=namespace-a")
	})
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

func (s *AdminSuite) TestListNamespaces() {
	s.Run("info_handler", func() {
		resp := listNamespaces(s.T())
		resp.Status(http.StatusOK).
			JSON().
			Equal(expectedListNamespaceResponse)
	})
}

func createNamespace(t *testing.T, namespaceName string, namespaceId int32) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(getCreateNamespaceURL(namespaceName)).
		WithJSON(AdminTestMap{"id": namespaceId}).
		Expect()
}

func listNamespaces(t *testing.T) *httpexpect.Response {
	e := adminExpect(t)
	return e.POST(listNamespaceUrl()).
		WithJSON(AdminTestMap{"id": ""}).
		Expect()
}

func getCreateNamespaceURL(namespaceName string) string {
	return fmt.Sprintf("/admin/v1/namespaces/%s/create", namespaceName)
}
func listNamespaceUrl() string {
	return "/admin/v1/namespaces/list"
}
