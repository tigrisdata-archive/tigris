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

package encoding

import (
	"github.com/tigrisdata/tigrisdb/keys"
	"github.com/tigrisdata/tigrisdb/schema"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"github.com/tigrisdata/tigrisdb/value"
	"google.golang.org/protobuf/types/known/structpb"
)

type Encoder interface {
	BuildKey(doc map[string]*structpb.Value, collection schema.Collection) (keys.Key, error)
}

type PrefixEncoder struct{}

func (e *PrefixEncoder) encodeKey(doc map[string]*structpb.Value, prefix string, userDefinedKeys []*schema.Field) (keys.Key, error) {
	var primaryKeyParts []interface{}
	for _, v := range userDefinedKeys {
		k, ok := doc[v.Name()]
		if !ok {
			return nil, ulog.CE("missing primary key column(s) %v", v)
		}

		val, err := value.NewValueUsingSchema(v, k)
		if err != nil {
			return nil, err
		}
		primaryKeyParts = append(primaryKeyParts, val.AsInterface())
	}
	if len(primaryKeyParts) == 0 {
		return nil, ulog.CE("missing primary key column(s)")
	}
	return keys.NewKey([]byte(prefix), primaryKeyParts...), nil
}

func (e *PrefixEncoder) BuildKey(doc map[string]*structpb.Value, collection schema.Collection) (keys.Key, error) {
	key, err := e.encodeKey(doc, collection.StorageName(), collection.PrimaryKeys())
	if err != nil {
		return nil, err
	}

	return key, nil
}
