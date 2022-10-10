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

package update

import (
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/util/log"
)

// FieldOPType is the field operator passed in the Update API.
type FieldOPType string

const (
	Set   FieldOPType = "$set"
	UnSet FieldOPType = "$unset"
)

// BuildFieldOperators un-marshals request "fields" present in the Update API and returns a FieldOperatorFactory
// The FieldOperatorFactory has the logic to remove/merge the JSON passed in the input and the one present in the
// database.
func BuildFieldOperators(reqFields []byte) (*FieldOperatorFactory, error) {
	var decodedOperators map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(reqFields, &decodedOperators); log.E(err) {
		return nil, err
	}

	operators := make(map[string]*FieldOperator)
	for op, val := range decodedOperators {
		if op == string(Set) {
			operators[string(Set)] = NewFieldOperator(Set, val)
		} else if op == string(UnSet) {
			operators[string(UnSet)] = NewFieldOperator(UnSet, val)
		}
	}

	return &FieldOperatorFactory{
		FieldOperators: operators,
	}, nil
}

// The FieldOperatorFactory has all the field operators passed in the Update API request. The factory implements a
// MergeAndGet method to convert the input to the output JSON that needs to be persisted in the database.
type FieldOperatorFactory struct {
	FieldOperators map[string]*FieldOperator
}

// MergeAndGet method to converts the input to the output after applying all the operators. First "$set" operation is
// applied and then "$unset" which means if a field is present in both $set and $unset then it won't be stored in the
// resulting document.
func (factory *FieldOperatorFactory) MergeAndGet(existingDoc jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	out := existingDoc
	var err error
	if setFieldOp, ok := factory.FieldOperators[string(Set)]; ok {
		if out, err = factory.set(out, setFieldOp.Input); err != nil {
			return nil, err
		}
	}
	if unsetFieldOp, ok := factory.FieldOperators[string(UnSet)]; ok {
		if out, err = factory.remove(out, unsetFieldOp.Input); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (factory *FieldOperatorFactory) remove(out jsoniter.RawMessage, toRemove jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	var unsetArray []string
	if err := jsoniter.Unmarshal(toRemove, &unsetArray); err != nil {
		return nil, err
	}

	for _, unset := range unsetArray {
		unsetKeys := strings.Split(unset, ".")
		out = jsonparser.Delete(out, unsetKeys...)
	}

	return out, nil
}

func (factory *FieldOperatorFactory) set(existingDoc jsoniter.RawMessage, setDoc jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	var (
		output []byte = existingDoc
		err    error
	)
	err = jsonparser.ObjectEach(setDoc, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if dataType == jsonparser.String {
			value = []byte(fmt.Sprintf(`"%s"`, value))
		}

		keys := strings.Split(string(key), ".")
		output, err = jsonparser.Set(output, value, keys...)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}

// A FieldOperator can be of the following type:
// { "$set": { <field1>: <value1>, ... } }
// { "$incr": { <field1>: <value> } }
// { "$unset": ["d"] }.
type FieldOperator struct {
	Op    FieldOPType
	Input jsoniter.RawMessage
}

// NewFieldOperator returns a FieldOperator.
func NewFieldOperator(op FieldOPType, val jsoniter.RawMessage) *FieldOperator {
	return &FieldOperator{
		Op:    op,
		Input: val,
	}
}
