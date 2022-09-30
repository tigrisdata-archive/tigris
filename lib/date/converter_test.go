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

package date

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToUnixNano(t *testing.T) {
	validCases := []struct {
		name     string
		date     string
		expected int64
	}{
		{"UTC RFC 3339", "2022-10-18T00:51:07+00:00", 1666054267000000000},
		{"UTC RFC 3339 Nano", "2022-10-18T00:51:07.528106+00:00", 1666054267528106000},
		{"IST RFC 3339", "2022-10-11T04:19:32+05:30", 1665442172000000000},
		{"IST RFC 3339 Nano", "2022-10-18T00:51:07.999999999+05:30", 1666034467999999999},
		{"No TZ RFC 3339", "2022-10-18T00:51:07Z", 1666054267000000000},
	}

	for _, v := range validCases {
		t.Run(v.name, func(t *testing.T) {
			actual, err := ToUnixNano(time.RFC3339Nano, v.date)
			assert.NoError(t, err)
			assert.Equal(t, v.expected, actual)
		})
	}

	failureCases := []struct {
		name      string
		date      string
		errorLike string
	}{
		{"RFC 1123", "Mon, 02 Jan 2006 15:04:05 MST", "cannot parse"},
	}

	for _, v := range failureCases {
		t.Run(v.name, func(t *testing.T) {
			_, err := ToUnixNano(time.RFC3339Nano, v.date)
			assert.ErrorContains(t, err, v.errorLike)
		})
	}
}

func TestToUnixMilli(t *testing.T) {
	validCases := []struct {
		name     string
		date     string
		expected int64
	}{
		{"UTC RFC 3339", "2022-10-18T00:51:07+00:00", 1666054267000},
		{"UTC RFC 3339 Nano", "2022-10-18T00:51:07.528106+00:00", 1666054267528},
		{"IST RFC 3339", "2022-10-11T04:19:32+05:30", 1665442172000},
		{"IST RFC 3339 Nano", "2022-10-18T00:51:07.999999999+05:30", 1666034467999},
		{"No TZ RFC 3339", "2022-10-18T00:51:07Z", 1666054267000},
	}

	for _, v := range validCases {
		t.Run(v.name, func(t *testing.T) {
			actual, err := ToUnixMilli(time.RFC3339Nano, v.date)
			assert.NoError(t, err)
			assert.Equal(t, v.expected, actual)
		})
	}

	failureCases := []struct {
		name      string
		date      string
		errorLike string
	}{
		{"RFC 1123", "Mon, 02 Jan 2006 15:04:05 MST", "cannot parse"},
	}

	for _, v := range failureCases {
		t.Run(v.name, func(t *testing.T) {
			_, err := ToUnixMilli(time.RFC3339Nano, v.date)
			assert.ErrorContains(t, err, v.errorLike)
		})
	}
}
