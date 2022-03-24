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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateCollectionFromSchema(t *testing.T) {
	reqSchema := []byte(`{"title":"Record of an order","description":"This document records the details of an order","properties":{"order_id":{"description":"A unique identifier for an order","type":"bigint"},"cust_id":{"description":"A unique identifier for a customer","type":"bigint"},"product":{"description":"name of the product","type":"string","max_length":100,"unique":true},"quantity":{"description":"number of products ordered","type":"int"},"price":{"description":"price of the product","type":"double"},"date_ordered":{"description":"The date order was made","type":"datetime"}},"primary_key":["cust_id","order_id"]}`)
	c, err := CreateCollection("d1", "t1", reqSchema)
	require.NoError(t, err)
	require.Equal(t, c.Name(), "t1")
	require.Equal(t, c.PrimaryKeys()[0].FieldName, "cust_id")
	require.Equal(t, c.PrimaryKeys()[1].FieldName, "order_id")
}
