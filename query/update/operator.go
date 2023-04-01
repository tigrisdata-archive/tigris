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

package update

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/util/log"
)

// FieldOPType is the field operator passed in the Update API.
type FieldOPType string

const (
	Set       FieldOPType = "$set"
	UnSet     FieldOPType = "$unset"
	Increment FieldOPType = "$increment"
	Decrement FieldOPType = "$decrement"
	Multiply  FieldOPType = "$multiply"
	Divide    FieldOPType = "$divide"
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
		switch op {
		case string(Set):
			operators[string(Set)] = NewFieldOperator(Set, val)
		case string(UnSet):
			operators[string(UnSet)] = NewFieldOperator(UnSet, val)
		case string(Increment):
			operators[string(Increment)] = NewFieldOperator(Increment, val)
		case string(Decrement):
			operators[string(Decrement)] = NewFieldOperator(Decrement, val)
		case string(Multiply):
			operators[string(Multiply)] = NewFieldOperator(Multiply, val)
		case string(Divide):
			operators[string(Divide)] = NewFieldOperator(Divide, val)
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
func (factory *FieldOperatorFactory) MergeAndGet(existingDoc jsoniter.RawMessage, collection *schema.DefaultCollection) (jsoniter.RawMessage, []string, bool, error) {
	primaryKeyMutation := false
	out := existingDoc
	var searchIndexesToRemove []string
	var err error
	if setFieldOp, ok := factory.FieldOperators[string(Set)]; ok {
		if out, searchIndexesToRemove, primaryKeyMutation, err = factory.set(collection, out, setFieldOp); err != nil {
			return nil, nil, false, err
		}
	}
	if incrFieldOp, ok := factory.FieldOperators[string(Increment)]; ok {
		if out, primaryKeyMutation, err = factory.atomicOperations(collection, out, incrFieldOp); err != nil {
			return nil, nil, false, err
		}
	}
	if decrFieldOp, ok := factory.FieldOperators[string(Decrement)]; ok {
		if out, primaryKeyMutation, err = factory.atomicOperations(collection, out, decrFieldOp); err != nil {
			return nil, nil, false, err
		}
	}
	if multFieldOp, ok := factory.FieldOperators[string(Multiply)]; ok {
		if out, primaryKeyMutation, err = factory.atomicOperations(collection, out, multFieldOp); err != nil {
			return nil, nil, false, err
		}
	}
	if divFieldOp, ok := factory.FieldOperators[string(Divide)]; ok {
		if out, primaryKeyMutation, err = factory.atomicOperations(collection, out, divFieldOp); err != nil {
			return nil, nil, false, err
		}
	}
	if unsetFieldOp, ok := factory.FieldOperators[string(UnSet)]; ok {
		if out, primaryKeyMutation, err = factory.remove(collection, out, unsetFieldOp); err != nil {
			return nil, nil, false, err
		}
		if primaryKeyMutation {
			return nil, nil, false, errors.InvalidArgument("primary key field can't be unset")
		}
	}

	return out, searchIndexesToRemove, primaryKeyMutation, nil
}

func isPrimaryKeyMutation(collection *schema.DefaultCollection, mutationKey string) bool {
	field := collection.GetField(mutationKey)
	return field != nil && field.IsPrimaryKey()
}

func (*FieldOperatorFactory) remove(collection *schema.DefaultCollection, out jsoniter.RawMessage, operator *FieldOperator) (jsoniter.RawMessage, bool, error) {
	var unsetArray []string
	if err := jsoniter.Unmarshal(operator.Input, &unsetArray); err != nil {
		return nil, false, err
	}

	primaryKeyMutation := false
	for _, unset := range unsetArray {
		unsetKeys := strings.Split(unset, ".")
		if !primaryKeyMutation {
			primaryKeyMutation = isPrimaryKeyMutation(collection, unsetKeys[0])
		}
		out = jsonparser.Delete(out, unsetKeys...)
	}

	return out, primaryKeyMutation, nil
}

func (factory *FieldOperatorFactory) set(collection *schema.DefaultCollection, existingDoc jsoniter.RawMessage, operator *FieldOperator) (jsoniter.RawMessage, []string, bool, error) {
	var (
		tentativeKeysToRemove []string
		output                []byte = existingDoc
		err                   error
	)

	primaryKeyMutation := false
	err = jsonparser.ObjectEach(operator.Input, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}
		if dataType == jsonparser.String {
			value = []byte(fmt.Sprintf(`"%s"`, value))
		}
		if dataType == jsonparser.Object {
			var tentativeList []string
			tentativeList, err = factory.buildKeysForObjects(existingDoc, key)
			if err != nil {
				return err
			}

			tentativeKeysToRemove = append(tentativeKeysToRemove, tentativeList...)
		}
		keys := strings.Split(string(key), ".")
		isPrimaryKeyMutation := isPrimaryKeyMutation(collection, keys[0])
		if isPrimaryKeyMutation && dataType == jsonparser.Null {
			return errors.InvalidArgument("primary key field can't be set as null")
		}

		if !primaryKeyMutation {
			primaryKeyMutation = isPrimaryKeyMutation
		}
		output, err = jsonparser.Set(output, value, keys...)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, nil, primaryKeyMutation, err
	}

	return output, tentativeKeysToRemove, primaryKeyMutation, nil
}

func (factory *FieldOperatorFactory) buildKeysForObjects(existingDoc []byte, key []byte) ([]string, error) {
	value, dt, _, _ := jsonparser.Get(existingDoc, string(key))
	if dt != jsonparser.Object {
		return nil, nil
	}

	var flattenedKeys []string
	return flattenedKeys, factory.buildKeysForObjectsInternal(string(key), value, &flattenedKeys)
}

func (factory *FieldOperatorFactory) buildKeysForObjectsInternal(parent string, existingObj []byte, result *[]string) error {
	var err error
	err = jsonparser.ObjectEach(existingObj, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		if err != nil {
			return err
		}

		if dataType == jsonparser.Object {
			return factory.buildKeysForObjectsInternal(parent+"."+string(key), value, result)
		}

		*(result) = append(*(result), parent+"."+string(key))
		return nil
	})
	return err
}

func (*FieldOperatorFactory) atomicOperations(collection *schema.DefaultCollection, existingDoc jsoniter.RawMessage, operator *FieldOperator) (jsoniter.RawMessage, bool, error) {
	var output []byte = existingDoc
	var atomicInput map[string]float64
	if err := jsoniter.Unmarshal(operator.Input, &atomicInput); err != nil {
		return nil, false, errors.InvalidArgument("invalid input '%s'", string(operator.Input))
	}

	primaryKeyMutation := false
	for key, value := range atomicInput {
		field, err := collection.GetQueryableField(key)
		if err != nil {
			return nil, false, err
		}

		keys := strings.Split(key, ".")
		existingVal, dataType, _, err := jsonparser.Get(existingDoc, keys...)
		if err != nil && dataType != jsonparser.NotExist {
			return nil, false, errors.Internal("failing to get key '%s' err: '%s'", keys, err.Error())
		}
		if dataType == jsonparser.NotExist {
			// If a field is null, it will not be updated
			continue
		}

		if !primaryKeyMutation {
			primaryKeyMutation = isPrimaryKeyMutation(collection, keys[0])
		}

		newValue, err := operator.apply(field.DataType, existingVal, value)
		if err != nil {
			return nil, false, err
		}

		output, err = jsonparser.Set(output, newValue, keys...)
		if err != nil {
			return nil, false, err
		}
	}

	return output, primaryKeyMutation, nil
}

// A FieldOperator can be of the following type:
// { "$set": { <field1>: <value1>, ... } }
// { "$increment": { <field1>: <incrementBy> } }
// { "$decrement": { <field1>: <decrementBy> } }
// { "$multiply": { <field1>: <multiplyBy> } }
// { "$divide": { <field1>: <divideBy> } }
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

func (operator *FieldOperator) apply(fieldType schema.FieldType, existingVal []byte, inputValue float64) ([]byte, error) {
	var output float64
	if existingVal != nil {
		var err error
		output, err = strconv.ParseFloat(string(existingVal), 64)
		if err != nil {
			return nil, errors.InvalidArgument(fmt.Errorf("unsupported value type: %w ", err).Error())
		}
	}

	switch fieldType {
	case schema.Int32Type, schema.Int64Type:
		if inputValue != float64(int64(inputValue)) {
			return nil, errors.InvalidArgument("floating operations are not allowed on integer field")
		}
	}

	switch operator.Op {
	case Increment:
		output += inputValue
	case Decrement:
		output -= inputValue
	case Multiply:
		output *= inputValue
	case Divide:
		if inputValue == 0 {
			return nil, errors.InvalidArgument("division by 0 is not allowed")
		}
		output /= inputValue
	default:
		return nil, errors.InvalidArgument("unsupported operator '%s' for atomic operation", operator.Op)
	}

	switch fieldType {
	case schema.Int32Type, schema.Int64Type:
		return jsoniter.Marshal(int64(output))
	case schema.DoubleType:
		return jsoniter.Marshal(output)
	}

	return nil, errors.InvalidArgument("field type '%s' not supporting atomic operation", schema.FieldNames[fieldType])
}
