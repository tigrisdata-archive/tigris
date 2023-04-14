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

package database

import (
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/sort"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/value"
)

type PrimaryIndexPlanner struct {
	noFilter  bool
	coll      *schema.DefaultCollection
	encFunc   filter.KeyEncodingFunc
	filter    []filter.Filter
	idxFields []*schema.QueryableField
}

func NewPrimaryIndexQueryPlanner(coll *schema.DefaultCollection, e metadata.Encoder, f []byte, c *value.Collation) (*PrimaryIndexPlanner, error) {
	planner := &PrimaryIndexPlanner{
		coll:      coll,
		idxFields: coll.GetPrimaryIndexedFields(),
	}

	planner.encFunc = func(indexParts ...any) (keys.Key, error) {
		return e.EncodeKey(coll.EncodedName, coll.GetPrimaryKey(), indexParts)
	}

	planner.noFilter = filter.None(f)
	if !planner.noFilter {
		filterFactory := filter.NewFactory(coll.QueryableFields, c)
		filters, err := filterFactory.Factorize(f)
		if err != nil {
			return nil, err
		}

		planner.filter = filters
	}

	return planner, nil
}

func (planner *PrimaryIndexPlanner) isPrefixSort(sortPlan *filter.QueryPlan) bool {
	return sortPlan != nil && sortPlan.FieldName == planner.idxFields[0].Name()
}

func (planner *PrimaryIndexPlanner) SortPlan(sorting *sort.Ordering) (*filter.QueryPlan, error) {
	return filter.QueryPlanFromSort(
		sorting,
		planner.idxFields,
		planner.encFunc,
		filter.PKBuildIndexPartsFunc,
		filter.PrimaryIndex,
	)
}

func (planner *PrimaryIndexPlanner) GenerateTablePlan(sortPlan *filter.QueryPlan, from keys.Key) (*filter.TableScanPlan, error) {
	reverse := false
	if sortPlan != nil {
		reverse = sortPlan.Reverse()
	}

	return &filter.TableScanPlan{
		Table:   planner.coll.EncodedName,
		From:    from,
		Reverse: reverse,
	}, nil
}

func (planner *PrimaryIndexPlanner) GeneratePlan(sortPlan *filter.QueryPlan, from keys.Key) (*filter.QueryPlan, error) {
	kb := filter.NewPrimaryKeyEqBuilder(planner.encFunc)
	plans, err := kb.Build(planner.filter, planner.coll.GetPrimaryIndexedFields())
	if err != nil {
		return nil, err
	}
	plan := plans[0]
	if sortPlan != nil {
		if !planner.isPrefixSort(sortPlan) {
			return nil, errors.InvalidArgument("sorting is only allowed on prefix in case of composite primary keys")
		}
		// this means one of the primary key is used for sorting
		plan.Ascending = sortPlan.Ascending
	}
	plan.From = from

	// Only zeroth is applicable
	return &plan, nil
}

// IsPrefixQueryWithSuffixSort returns if it is prefix query with sort can be on suffix primary key field
// ToDo: This needs to be moved inside builder where we need to build prefix keys as well.
func (planner *PrimaryIndexPlanner) IsPrefixQueryWithSuffixSort(sortPlan *filter.QueryPlan) bool {
	if sortPlan == nil {
		return false
	}
	if len(planner.filter) == 0 {
		return false
	}
	if len(planner.idxFields) == 1 && sortPlan.FieldName == planner.idxFields[0].Name() {
		// shortcut if sorting is on primary key field and there is just single
		// primary key field. Reason being as this can be simply be a sort on the
		// primary key.
		return true
	}

	var index int
	for _, idxField := range planner.idxFields {
		if sortPlan.FieldName == idxField.Name() {
			break
		}
		index++
	}

	prefixFields := container.NewHashSet()
	for j := 0; j < index; j++ {
		prefixFields.Insert(planner.idxFields[j].Name())
	}
	if prefixFields.Length() == 0 {
		return false
	}

	expLength := prefixFields.Length()
	filters := planner.filter
	if len(planner.filter) == 1 {
		if l, ok := planner.filter[0].(filter.LogicalFilter); ok {
			filters = l.GetFilters()
			if l.Type() == filter.OrOP {
				expLength *= len(filters)
			}
		}
	}

	found := 0
	for _, f := range filters {
		switch ss := f.(type) {
		case *filter.Selector:
			if prefixFields.Contains(ss.Field.Name()) && ss.Matcher.Type() == filter.EQ {
				found++
			}
		default:
			return false
		}
	}

	return found == expLength
}
