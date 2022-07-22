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
	s.Run("list_namespaces", func() {
		_ = createNamespace(s.T(), "namespace-b", 3)
		resp := listNamespaces(s.T())
		namespaces := resp.Status(http.StatusOK).
			JSON().
			Object().
			Value("namespaces").
			Array().
			Raw()
		var found = false
		for _, namespace := range namespaces {
			if converted, ok := namespace.(map[string]interface{}); ok {
				if converted["name"] == "namespace-b" {
					found = true
				}
			}
		}
		s.True(found)
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
