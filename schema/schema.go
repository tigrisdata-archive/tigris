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
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func CreateCollectionFromSchema(database string, collection string, userSchema map[string]*structpb.Value) (Collection, error) {
	var fields []*Field
	var nameToFieldMapping = make(map[string]*Field)
	for key, value := range userSchema {
		if key == PrimaryKeySchemaName {
			continue
		}

		f := NewField(key, value.GetStringValue(), false)
		fields = append(fields, f)
		nameToFieldMapping[f.FieldName] = f
	}

	var primaryKeyFields []*Field
	v := userSchema[PrimaryKeySchemaName]
	if list := v.GetListValue(); list != nil {
		for _, l := range list.GetValues() {
			f, ok := nameToFieldMapping[l.GetStringValue()]
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "missing primary key '%s' field in schema", l.GetStringValue())
			}

			ptrTrue := true
			f.PrimaryKeyField = &ptrTrue

			primaryKeyFields = append(primaryKeyFields, f)
		}
	}

	return NewCollection(database, collection, fields, primaryKeyFields), nil
}

func ExtractKeysFromSchema(userSchema map[string]*structpb.Value) ([]*Field, error) {
	var keys []*Field
	v := userSchema[PrimaryKeySchemaName]
	if list := v.GetListValue(); list != nil {
		for _, l := range list.GetValues() {
			keys = append(keys, NewField(l.GetStringValue(), stringDef, true))
		}
		return keys, nil
	} else {
		return nil, fmt.Errorf("not compatible keys")
	}
}

func StorageName(databaseName, collectionName string) string {
	return fmt.Sprintf("%s.%s", databaseName, collectionName)
}
