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
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/schema"
)

/*
https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/#std-label-bson-types-comparison-order
MinKey (internal type)
Null
Numbers (ints, longs, doubles, decimals)
Symbol, String
Object
Array
BinData
ObjectId
Boolean - false then true
Date
Timestamp
Regular Expression
MaxKey (internal type)

*/

func SecondaryMinOrder() int {
	return -1
}

func SecondaryMaxOrder() int {
	return 127
}

func SecondaryNullOrder() int {
	return ToSecondaryOrder(schema.NullType, nil)
}

func ToSecondaryOrder(dataType schema.FieldType, val Value) int {
	switch dataType {
	case schema.NullType:
		return 5
	case schema.DoubleType, schema.Int32Type, schema.Int64Type:
		return 10
	case schema.StringType, schema.UUIDType:
		return 15
	case schema.ObjectType:
		return 20
	case schema.BoolType:
		if val != nil && val.String() == "true" {
			return 26
		}
		return 25
	case schema.DateTimeType:
		return 30
	case schema.MaxType:
		return SecondaryMaxOrder()
	}

	log.Info().Msgf("Count not order type for index %d", dataType)
	return 35
}
