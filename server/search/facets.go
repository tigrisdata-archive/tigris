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

package search

import (
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/lib/container"
	tsApi "github.com/typesense/typesense-go/typesense/api"
	"github.com/pkg/errors"
	api "github.com/tigrisdata/tigris/api/server/v1"
)

// SortedFacets is a Temporary workaround to merge facet values when aggregating results from
// multi-search queries resulting from OR filters
// this is not very efficient as of now
type SortedFacets struct {
	counts     map[string]*container.PriorityQueue[FacetCount]
	facetAttrs map[string]*FacetAttrs
	sorted     bool
}

func NewSortedFacets() *SortedFacets {
	return &SortedFacets{
		counts:     map[string]*container.PriorityQueue[FacetCount]{},
		facetAttrs: map[string]*FacetAttrs{},
		sorted:     false,
	}
}

// Add creates or merges the facet counts with existing for each field
// new values cannot be added to this data structure once it has been sorted
func (f *SortedFacets) Add(tsCounts *tsApi.FacetCounts) error {
	if tsCounts == nil || tsCounts.FieldName == nil {
		return nil
	}
	if f.sorted {
		return errors.New("Already initialized and sorted. No more inserts.")
	}

	if _, ok := f.facetAttrs[*tsCounts.FieldName]; !ok {
		f.facetAttrs[*tsCounts.FieldName] = newFacetAttrs()
	}
	attrs := f.facetAttrs[*tsCounts.FieldName]

	for i := 0; tsCounts.Counts != nil && i < len(*tsCounts.Counts); i++ {
		c := (*tsCounts.Counts)[i]
		if c.Value != nil {
			attrs.addCount(*c.Value, c.Count)
		}
	}

	attrs.addStats(tsCounts)
	return nil
}

// GetFacetCount removes from priority queue and returns the value with highest count for the field if present
// else returns nil, False
func (f *SortedFacets) GetFacetCount(field string) (*FacetCount, bool) {
	if !f.sorted {
		f.sort()
	}
	if f.hasMoreFacets(field) {
		if fc, err := f.counts[field].Pop(); err == nil {
			return fc, true
		} else {
			log.Err(err)
			return nil, false
		}
	}

	return nil, false
}

// GetStats returns the computed stats for the faceted field
func (f *SortedFacets) GetStats(field string) *api.FacetStats {
	if attrs, ok := f.facetAttrs[field]; ok {
		return attrs.stats
	} else {
		return nil
	}
}

func (f *SortedFacets) hasMoreFacets(field string) bool {
	if q, ok := f.counts[field]; ok {
		return q.Len() > 0
	}
	return false
}

func (f *SortedFacets) initPriorityQueue(field string) {
	if _, ok := f.counts[field]; !ok {
		f.counts[field] = container.NewPriorityQueue[FacetCount](facetCountComparator)
	}
}

// sort will queue up the collected unique facet counts in priority queue
func (f *SortedFacets) sort() {
	if f.sorted {
		return
	}

	for field, attrs := range f.facetAttrs {
		f.initPriorityQueue(field)
		for _, fc := range attrs.counts {
			if err := f.counts[field].Push(fc); err != nil {
				log.Err(err)
			}
		}
	}
	f.sorted = true
}

type FacetAttrs struct {
	counts         map[string]*FacetCount
	stats          *api.FacetStats
	statsBuiltOnce bool
}

func (fa *FacetAttrs) addCount(value string, count *int) {
	if fc, ok := fa.counts[value]; ok {
		fc.Count += int64(*count)
	} else {
		fa.counts[value] = &FacetCount{
			Value: value,
			Count: int64(*count),
		}
	}
}

// adds stats to existing FacetAttrs
func (fa *FacetAttrs) addStats(counts *tsApi.FacetCounts) {
	if counts == nil || counts.Stats == nil {
		return
	}

	// always increment the facet counts, although the values could be incorrect
	if counts.Stats.TotalValues != nil {
		fa.stats.Count += int64(*counts.Stats.TotalValues)
	}

	// reset all stats to nil when using multi-queries, the values cannot be computed correctly
	if fa.statsBuiltOnce {
		fa.stats.Avg = nil
		fa.stats.Min = nil
		fa.stats.Max = nil
		fa.stats.Sum = nil
	} else {
		// build stats
		if counts.Stats.Avg != nil {
			avg := float64(*counts.Stats.Avg)
			fa.stats.Avg = &avg
		}
		if counts.Stats.Max != nil {
			max := float64(*counts.Stats.Max)
			fa.stats.Max = &max
		}
		if counts.Stats.Min != nil {
			min := float64(*counts.Stats.Min)
			fa.stats.Min = &min
		}
		if counts.Stats.Sum != nil {
			sum := float64(*counts.Stats.Sum)
			fa.stats.Sum = &sum
		}
		fa.statsBuiltOnce = true
	}
}

func newFacetAttrs() *FacetAttrs {
	return &FacetAttrs{
		counts: map[string]*FacetCount{},
		stats:  &api.FacetStats{},
	}
}

type FacetCount struct {
	Value string
	Count int64
}

// facetCountComparator returns True if `this` needs to be sorted before `that`
func facetCountComparator(this, that *FacetCount) bool {
	if this == nil {
		return that == nil
	} else if that == nil {
		return this != nil
	}

	return this.Count > that.Count
}
