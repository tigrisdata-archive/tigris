package server

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris/test/config"
	"gopkg.in/gavv/httpexpect.v1"
)

type AdminTestMap map[string]interface{}

func TestCreateNamespace(t *testing.T) {
	name := fmt.Sprintf("namespace-a-%x", rand.Int63())
	id := rand.Int31()
	resp := createNamespace(t, name, id)
	resp.Status(http.StatusOK).
		JSON().
		Object().
		ValueEqual("message", fmt.Sprintf("Namespace created, with id=%d, and name=%s", id, name))
}

func adminExpect(s httpexpect.LoggerReporter) *httpexpect.Expect {
	return httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  config.GetBaseURL(),
		Reporter: httpexpect.NewAssertReporter(s),
	})
}

func TestListNamespaces(t *testing.T) {
	name := fmt.Sprintf("namespace-b-%x", rand.Int63())
	id := rand.Int31()
	_ = createNamespace(t, name, id)
	resp := listNamespaces(t)
	namespaces := resp.Status(http.StatusOK).
		JSON().
		Object().
		Value("namespaces").
		Array().
		Raw()
	var found = false
	for _, namespace := range namespaces {
		if converted, ok := namespace.(map[string]interface{}); ok {
			if converted["name"] == name {
				found = true
				assert.Equal(t, float64(id), converted["id"])
			}
		}
	}
	assert.True(t, found)
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
