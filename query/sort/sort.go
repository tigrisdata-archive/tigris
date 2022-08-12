package sort

import (
	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

// TODO: Update this to 3 once https://github.com/typesense/typesense/issues/690 is resolved
const maxSortOrders = 2

const (
	ASC  = "$asc"
	DESC = "$desc"
)

type Ordering = []SortField

type SortField struct {
	// Required; Name of field to enable sorting for
	Name string
	// Required; True if ascending order is requested, False for descending
	Ascending bool
	// Optional; True if missing/empty/null values to be presented at the top of sort order,
	// else they are sorted to the end by default
	MissingValuesFirst bool
}

func newSortField(order jsoniter.RawMessage) (SortField, error) {
	var s SortField
	err := jsonparser.ObjectEach(order, func(k []byte, v []byte, vt jsonparser.ValueType, offset int) error {
		switch string(v) {
		case ASC:
			s.Ascending = true
		case DESC:
			s.Ascending = false
		default:
			return api.Errorf(api.Code_INVALID_ARGUMENT, "Sort order can only be `%s` or `%s`", ASC, DESC)
		}
		s.Name = string(k)
		s.MissingValuesFirst = false // Forcing empty/null/missing values to the end
		return nil
	})
	if err != nil {
		return s, err
	}
	return s, nil
}

// UnmarshalSort expects a json array input. Examples:
//	[{"field_1": "$asc"}, {"field_2": "$desc"}]
//	[]
func UnmarshalSort(input jsoniter.RawMessage) (*Ordering, error) {
	if len(input) == 0 {
		return nil, nil
	}

	var orders = Ordering{}
	var err error
	_, err2 := jsonparser.ArrayEach(input, func(item []byte, vt jsonparser.ValueType, offset int, err1 error) {
		if err1 != nil {
			err = err1
			return
		}

		if vt != jsonparser.Object {
			err = api.Errorf(api.Code_INVALID_ARGUMENT, "Invalid value for `%s`", "sort")
			return
		}

		if len(orders) >= maxSortOrders {
			err = api.Errorf(api.Code_INVALID_ARGUMENT, "Sorting can support up to `%d` fields only", maxSortOrders)
			return
		}

		var f SortField
		f, err = newSortField(item)
		if err != nil {
			return
		}
		orders = append(orders, f)
	})

	if err != nil {
		return nil, err
	}

	if err2 != nil {
		return nil, err2
	}

	return &orders, nil
}
