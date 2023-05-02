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

package value

import (
	"strings"
)

func anyToInt(element any) (int64, bool) {
	if e, ok := element.(int64); ok {
		return e, true
	}
	if e, ok := element.(int); ok {
		return int64(e), true
	}
	if e, ok := element.(float64); ok && e == float64(int(e)) {
		return int64(e), true
	}
	return -1, false
}

func anyToFloat(element any) (float64, bool) {
	if e, ok := element.(float64); ok {
		return e, true
	}
	if e, ok := element.(int64); ok {
		return float64(e), true
	}
	if e, ok := element.(int); ok {
		return float64(e), true
	}
	return -1, false
}

func AnyCompare(element any, v Value) int {
	switch converted := v.(type) {
	case *StringValue:
		if e, ok := element.(string); ok {
			cmp := strings.Compare(e, converted.Value)
			return cmp
		}
	case *IntValue:
		if e, ok := anyToInt(element); ok {
			ev := IntValue(e)
			cmp, _ := ev.CompareTo(v)
			return cmp
		}
	case *DoubleValue:
		if e, ok := anyToFloat(element); ok {
			if e == converted.Double {
				return 0
			} else if e < converted.Double {
				return -1
			}
			return 1
		}
	case *DateTimeValue:
		if e, ok := element.(string); ok {
			return strings.Compare(e, converted.Value)
		}
	case *BoolValue:
		if e, ok := element.(bool); ok {
			ev := BoolValue(e)
			cmp, _ := ev.CompareTo(v)
			return cmp
		}
	case *NullValue:
		if element == converted {
			return 0
		}
		return -1
	}

	return -2
}
