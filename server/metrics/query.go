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

package metrics

import (
	"context"
	"strconv"

	ulog "github.com/tigrisdata/tigris/util/log"
)

type StreamingQueryMetrics struct {
	readType string
	isSort   bool
}

type SearchQueryMetrics struct {
	searchType string
	isSort     bool
}

type WriteQueryMetrics struct {
	writeType string
}

type QueryMetrics interface {
	GetTags() map[string]string
}

func (s *StreamingQueryMetrics) GetTags() map[string]string {
	return map[string]string{
		"read_type": s.readType,
		"sort":      strconv.FormatBool(s.isSort),
	}
}

func (s *StreamingQueryMetrics) SetReadType(value string) {
	s.readType = value
}

func (s *StreamingQueryMetrics) SetSort(value bool) {
	s.isSort = value
}

func UpdateSpanTags(ctx context.Context, qm QueryMetrics) context.Context {
	measurement, exists := MeasurementFromContext(ctx)
	if !exists {
		// No instrumentation on upper levels
		return ctx
	}
	measurement.RecursiveAddTags(qm.GetTags())
	resCtx, err := measurement.SaveMeasurementToContext(ctx)
	if err != nil {
		ulog.E(err)
	}
	return resCtx
}

func (s *SearchQueryMetrics) GetTags() map[string]string {
	return map[string]string{
		"search_type": s.searchType,
		"sort":        strconv.FormatBool(s.isSort),
	}
}

func (s *SearchQueryMetrics) SetSearchType(value string) {
	s.searchType = value
}

func (s *SearchQueryMetrics) SetSort(value bool) {
	s.isSort = value
}

func (w *WriteQueryMetrics) GetTags() map[string]string {
	return map[string]string{
		"write_type": w.writeType,
	}
}

func (w *WriteQueryMetrics) SetWriteType(value string) {
	w.writeType = value
}
