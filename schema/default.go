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

package schema

import (
	"encoding/json"
	"strings"

	"github.com/lucsky/cuid"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/uuid"
)

const (
	funcNowName  = "now()"
	funcUUIDName = "uuid()"
	funcCUIDName = "cuid()"
)

func isSupportedDefaultFunction(fnName string) bool {
	switch fnName {
	case funcNowName, funcUUIDName, funcCUIDName:
		return true
	}

	return !strings.HasSuffix(fnName, "()")
}

func getDefaultValue(fieldType FieldType) interface{} {
	switch fieldType {
	case BoolType:
		return false
	case Int32Type, Int64Type, DoubleType:
		return 0
	}

	return nil
}

type FieldDefaulter struct {
	value     interface{}
	createdAt bool
	updatedAt bool
}

func newDefaulter(createdAt *bool, updatedAt *bool, name string, dataType FieldType, v interface{}) (*FieldDefaulter, error) {
	if createdAt != nil && *createdAt {
		return &FieldDefaulter{
			createdAt: true,
		}, nil
	}
	if updatedAt != nil && *updatedAt {
		return &FieldDefaulter{
			updatedAt: true,
		}, nil
	}

	defaulter := &FieldDefaulter{}
	if v == "" {
		defaulter.value = getDefaultValue(dataType)
		return defaulter, nil
	}

	var err error
	switch ty := v.(type) {
	case bool:
		if dataType != BoolType {
			return nil, errors.InvalidArgument("default value should be true/false only for boolean field '%s'", name)
		}
	case json.Number:
		if dataType == Int32Type || dataType == Int64Type {
			if _, err = ty.Int64(); err != nil {
				return nil, errors.InvalidArgument("default value should be numeric for numeric field '%s'", name)
			}
		}
		if dataType == DoubleType {
			if _, err = ty.Float64(); err != nil {
				return nil, errors.InvalidArgument("default value should be numeric for numeric field '%s' %s", name, err.Error())
			}
		}
	case string:
		if dataType != StringType && dataType != DateTimeType && dataType != UUIDType && dataType != ByteType {
			return nil, errors.InvalidArgument("default value is not supported for '%s' type: '%s'", name, FieldNames[dataType])
		}
		if defaulter.createdAt && dataType != DateTimeType {
			return nil, errors.InvalidArgument("createdAt is only supported for date-time type")
		}
		if defaulter.updatedAt && dataType != DateTimeType {
			return nil, errors.InvalidArgument("updated is only supported for date-time type")
		}
		if v.(string) == funcNowName && dataType != DateTimeType {
			return nil, errors.InvalidArgument("now() is only supported for date-time type")
		}
		if !isSupportedDefaultFunction(v.(string)) {
			return nil, errors.InvalidArgument("'%s' function is not supported", v)
		}
		if v.(string) == funcNowName {
			// if it is now() then we can just set createdAt
			defaulter.createdAt = true
			v = nil
		}
	case []any:
		if dataType != ArrayType {
			return nil, errors.InvalidArgument("default value is not supported for '%s' type: '%s'", name, FieldNames[dataType])
		}
	default:
		return nil, errors.InvalidArgument("unsupported default value for field '%s' type: '%s'", name, FieldNames[dataType])
	}
	if err != nil {
		return nil, err
	}

	defaulter.value = v

	return defaulter, nil
}

func (defaulter *FieldDefaulter) TaggedWithUpdatedAt() bool {
	return defaulter.updatedAt
}

func (defaulter *FieldDefaulter) TaggedWithCreatedAt() bool {
	return defaulter.createdAt
}

// GetValue returns the value if there is default tag set for a field. For functions, it will execute it and return the
// value. If the function is now() then it is converted to createdAt during the construction of the defaulter.
func (defaulter *FieldDefaulter) GetValue() interface{} {
	switch defaulter.value {
	case funcUUIDName:
		return uuid.New().String()
	case funcCUIDName:
		return cuid.New()
	default:
		return defaulter.value
	}
}
