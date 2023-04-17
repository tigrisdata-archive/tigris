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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/value"
)

func TestNewMatcher(t *testing.T) {
	matcher, err := NewMatcher(EQ, value.NewIntValue(1))
	require.NoError(t, err)

	_, ok := matcher.(*EqualityMatcher)
	require.True(t, ok)

	matcher, err = NewMatcher("foo", value.NewIntValue(1))
	require.Equal(t, errors.InvalidArgument("unsupported operand 'foo'"), err)
	require.Nil(t, matcher)
}

func TestLikeMatcher(t *testing.T) {
	t.Run("regex", func(t *testing.T) {
		cases := []struct {
			input    any
			regex    string
			expMatch bool
			expError string
		}{
			{
				"foo bar",
				"foo",
				true,
				"",
			}, {
				"foo bar",
				"FOO",
				false,
				"",
			}, {
				"foo bar",
				"(?i)FOO",
				true,
				"",
			}, {
				[]string{"foo", "bar"},
				"bar",
				true,
				"",
			}, {
				[]string{"foo", "bar"},
				"bbr",
				false,
				"",
			}, {
				[]string{"foo", "bar"},
				"(?ci)FOO",
				false,
				"invalid or unsupported Perl syntax",
			},
		}
		for _, c := range cases {
			r, err := NewRegexMatcher(c.regex, value.NewCollation())
			if len(c.expError) > 0 {
				require.ErrorContains(t, err, c.expError)
				continue
			}
			require.NoError(t, err)
			require.Equal(t, c.expMatch, r.Matches(c.input))
		}
	})
	t.Run("contains", func(t *testing.T) {
		cases := []struct {
			input    any
			substr   string
			expMatch bool
			expError string
		}{
			{
				"foo bar",
				"foo",
				true,
				"",
			}, {
				"foo bar",
				"FOO",
				false,
				"",
			}, {
				[]string{"foo", "bar"},
				"bar",
				true,
				"",
			}, {
				[]string{"foo", "bar"},
				"bbr",
				false,
				"",
			},
		}
		for _, c := range cases {
			r, err := NewContainsMatcher(c.substr, value.NewCollation())
			if len(c.expError) > 0 {
				require.ErrorContains(t, err, c.expError)
				continue
			}
			require.NoError(t, err)
			require.Equal(t, c.expMatch, r.Matches(c.input))
		}
	})
	t.Run("not", func(t *testing.T) {
		cases := []struct {
			input    any
			substr   string
			expMatch bool
			expError string
		}{
			{
				"foo bar",
				"foo",
				false,
				"",
			}, {
				"foo bar",
				"FOO",
				true,
				"",
			}, {
				[]string{"foo", "bar"},
				"bar",
				false,
				"",
			}, {
				[]string{"foo", "bar"},
				"bbr",
				true,
				"",
			},
		}
		for _, c := range cases {
			r, err := NewNotMatcher(c.substr, value.NewCollation())
			if len(c.expError) > 0 {
				require.ErrorContains(t, err, c.expError)
				continue
			}
			require.NoError(t, err)
			require.Equal(t, c.expMatch, r.Matches(c.input))
		}
	})
}