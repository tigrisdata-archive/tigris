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

package schema

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/tigrisdata/tigris/templates"
)

var ErrUnsupportedType = fmt.Errorf("unsupported type")

type JSONToGo struct{}

func getGoStringType(format string) string {
	switch format {
	case formatDateTime:
		return "time.Time"
	case formatByte:
		return "[]byte"
	case formatUUID:
		return "uuid.UUID"
	default:
		return "string"
	}
}

func (*JSONToGo) GetType(tp string, format string) (string, error) {
	var resType string

	switch tp {
	case typeString:
		return getGoStringType(format), nil
	case typeInteger:
		switch format {
		case formatInt32:
			resType = "int32"
		default:
			resType = "int64"
		}
	case typeNumber:
		resType = "float64"
	case typeBoolean:
		resType = "bool"
	}

	if resType == "" {
		return "", errors.Wrapf(ErrUnsupportedType, "type=%s, format=%s", tp, format)
	}

	return resType, nil
}

func (*JSONToGo) GetObjectTemplate() string {
	return templates.SchemaGoObject
}
