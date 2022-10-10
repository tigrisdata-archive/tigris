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

package v1

import (
	"strconv"
	"strings"

	"github.com/tigrisdata/tigris/schema"
)

type payloadMutator struct {
	collection *schema.DefaultCollection
	mutated    bool
}

func newPayloadMutator(collection *schema.DefaultCollection) *payloadMutator {
	return &payloadMutator{
		collection: collection,
		mutated:    false,
	}
}

func (p *payloadMutator) isMutated() bool {
	return p.mutated
}

func (p *payloadMutator) convertStringToInt64(doc map[string]any) error {
	for key := range p.collection.GetInt64FieldsPath() {
		keys := strings.Split(key, ".")
		value, ok := doc[keys[0]]
		if !ok {
			continue
		}

		field := p.collection.GetField(keys[0])
		if field == nil {
			continue
		}

		if err := p.traverse(doc, value, keys[1:], field); err != nil {
			return err
		}
	}

	return nil
}

func (p *payloadMutator) traverse(parentMap map[string]any, value any, keys []string, parentField *schema.Field) error {
	var err error
	if parentField.Type() == schema.Int64Type {
		if conv, ok := value.(string); ok {
			if parentMap[parentField.FieldName], err = strconv.ParseInt(conv, 10, 64); err != nil {
				return err
			}
			p.mutated = true
			return nil
		}
	}

	switch converted := value.(type) {
	case map[string]any:
		value, ok := converted[keys[0]]
		if !ok {
			return nil
		}

		return p.traverse(converted, value, keys[1:], parentField.GetNestedField(keys[0]))
	case []any:
		// array should have a single nested field either as object or primitive type
		field := parentField.Fields[0]
		if field.DataType == schema.ObjectType {
			// is object or simple type
			for _, va := range converted {
				if err := p.traverse(va.(map[string]any), va, keys, field); err != nil {
					return err
				}
			}
		} else if field.DataType == schema.Int64Type {
			for idx := range converted {
				if conv, ok := converted[idx].(string); ok {
					if converted[idx], err = strconv.ParseInt(conv, 10, 64); err != nil {
						return err
					}
					p.mutated = true
				}
			}
		}
	}

	return nil
}
