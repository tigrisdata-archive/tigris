package update

import (
	"fmt"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc/codes"
)

// FieldOPType is the field operator passed in the Update API
type FieldOPType string

const (
	set FieldOPType = "$set"
)

// BuildFieldOperators un-marshals request "fields" present in the Update API and returns a FieldOperatorFactory
// The FieldOperatorFactory has the logic to remove/merge the JSON passed in the input and the one present in the
// database.
func BuildFieldOperators(reqFields []byte) (*FieldOperatorFactory, error) {
	var decodedOperators map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(reqFields, &decodedOperators); log.E(err) {
		return nil, err
	}

	var operators = make(map[string]*FieldOperator)
	for op, val := range decodedOperators {
		switch op {
		case string(set):
			operators[string(set)] = NewFieldOperator(set, val)
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

// MergeAndGet method to converts the input to the output after applying all the operators.
func (factory *FieldOperatorFactory) MergeAndGet(existingDoc jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	setFieldOp := factory.FieldOperators[string(set)]
	if setFieldOp == nil {
		return nil, api.Error(codes.InvalidArgument, "set operator not present in the fields parameter")
	}
	out, err := factory.apply(existingDoc, setFieldOp.Document)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (factory *FieldOperatorFactory) apply(input jsoniter.RawMessage, setDoc jsoniter.RawMessage) (jsoniter.RawMessage, error) {
	var (
		output []byte = input
		err    error
	)
	err = jsonparser.ObjectEach(setDoc, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch dataType {
		case jsonparser.String:
			value = []byte(fmt.Sprintf(`"%s"`, value))
		}
		output, err = jsonparser.Set(output, value, string(key))
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
// { "$remove": ["d"] }
type FieldOperator struct {
	Op       FieldOPType
	Document jsoniter.RawMessage
}

// NewFieldOperator returns a FieldOperator
func NewFieldOperator(op FieldOPType, val jsoniter.RawMessage) *FieldOperator {
	return &FieldOperator{
		Op:       op,
		Document: val,
	}
}
