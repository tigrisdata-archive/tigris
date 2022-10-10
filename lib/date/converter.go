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

import "time"

// ToUnixNano converts a string formatted time to Unix nanoseconds.
func ToUnixNano(format string, dateStr string) (int64, error) {
	t, err := time.Parse(format, dateStr)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

// ToUnixMilli converts a string formatted time to Unix milliseconds.
func ToUnixMilli(format string, dateStr string) (int64, error) {
	t, err := time.Parse(format, dateStr)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}
