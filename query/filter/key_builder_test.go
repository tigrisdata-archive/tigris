package filter

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/keys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testFilters(t testing.TB, input []byte) []Filter {
	var r = &api.ReadRequest{}
	require.NoError(t, json.Unmarshal(input, r))

	filters, err := Build(r.Filter)
	require.NoError(t, err)
	require.NotNil(t, filters)

	return filters
}

func TestKeyBuilder(t *testing.T) {
	cases := []struct {
		userKeys  []string
		userInput []byte
		expError  error
		expKeys   []keys.Key
	}{
		{
			// fewer fields in user input
			[]string{"a", "c", "b"},
			[]byte(`{"filter": [{"a": 10}, {"c": 10}]}`),
			status.Errorf(codes.InvalidArgument, "filters doesn't contains primary key fields"),
			nil,
		},
		{
			// some fields are not with eq
			[]string{"a", "c", "b"},
			[]byte(`{"filter": [{"a": 10}, {"b": {"$eq": 10}}, {"c": {"$gt": 15}}]}`),
			status.Errorf(codes.InvalidArgument, "filters only supporting $eq comparison, found '$gt'"),
			nil,
		},
		{
			// some fields are repeated
			[]string{"a", "b"},
			[]byte(`{"filter": [{"a": 10}, {"b": {"$eq": 10}}, {"b": 15}]}`),
			status.Errorf(codes.InvalidArgument, "reusing same fields for conditions on equality"),
			nil,
		},
		{
			// single user defined key
			[]string{"a"},
			[]byte(`{"filter": [{"b": 10}, {"a": {"$eq": 1}}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1))},
		},
		{
			// composite user defined key
			[]string{"a", "b", "c"},
			[]byte(`{"filter": [{"b": 10}, {"a": {"$eq": true}}, {"c": "foo"}]}`),
			nil,
			[]keys.Key{keys.NewKey("", true, int64(10), "foo")},
		},
		{
			// single with AND/OR filter
			[]string{"a"},
			[]byte(`{"filter": [{"$or": [{"a": 1}, {"$and": [{"a":2}, {"f1": 3}]}]}, {"$and": [{"a": 4}, {"$or": [{"a":5}, {"f2": 6}]}, {"$or": [{"a":5}, {"a": 6}]}]}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1)), keys.NewKey("", int64(4)), keys.NewKey("", int64(2)), keys.NewKey("", int64(5)), keys.NewKey("", int64(5)), keys.NewKey("", int64(6))},
		},
		{
			// composite with AND filter
			[]string{"a", "b"},
			[]byte(`{"filter":[{"$and":[{"a":1},{"b":"aaa"},{"$and":[{"a":2},{"c":5},{"b":"bbb"}]}]},{"$and":[{"a":4},{"c":10},{"b":"ccc"}]}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1), "aaa"), keys.NewKey("", int64(4), "ccc"), keys.NewKey("", int64(2), "bbb")},
		}, {
			[]string{"a"},
			[]byte(`{"filter":[{"b":10},{"a":1},{"c":"ccc"},{"$or":[{"f1":10},{"a":2}]}]}`),
			nil,
			[]keys.Key{keys.NewKey("", int64(1)), keys.NewKey("", int64(2))},
		},
	}
	for _, c := range cases {
		b := NewKeyBuilder(NewStrictEqKeyComposer(""))
		filters := testFilters(t, c.userInput)
		buildKeys, err := b.Build(filters, c.userKeys)
		require.Equal(t, c.expError, err)
		require.Equal(t, c.expKeys, buildKeys)
	}
}

func BenchmarkStrictEqKeyComposer_Compose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		kb := NewKeyBuilder(NewStrictEqKeyComposer(""))
		filters := testFilters(b, []byte(`{"filter": [{"b": 10}, {"a": {"$eq": 10}}, {"c": "foo"}]}`))
		_, err := kb.Build(filters, []string{"a", "b", "c"})
		require.NoError(b, err)
	}
	b.ReportAllocs()
}
