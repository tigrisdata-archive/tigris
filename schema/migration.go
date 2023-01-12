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

package schema

import (
	ljson "github.com/tigrisdata/tigris/lib/json"
	ulog "github.com/tigrisdata/tigris/util/log"
)

// Version is schema associated with it version.
type Version struct {
	Version int
	Schema  []byte
}

// Versions is an array of all collections schemas. Sorted in ascending schema version order.
type Versions []Version

func (x Versions) Len() int           { return len(x) }
func (x Versions) Less(i, j int) bool { return x[i].Version < x[j].Version }
func (x Versions) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (x Versions) Latest() Version { return x[len(x)-1] }

// VersionDeltaField describes field schema change.
//
// If field is deleted then To is equal to `UnknownType`.
// If field is added then From is equal to `Unknowntype`.
type VersionDeltaField struct {
	KeyPath []string  // Key path of the field
	From    FieldType // The type is changing from this
	To      FieldType // to this
}

// VersionDelta contains all fields schema changes in particular schema version.
type VersionDelta struct {
	version int32
	fields  []VersionDeltaField
}

// buildSchemaDeltas builds all the schema deltas from the collection schema changes history.
func buildSchemaDeltas(_ Versions) []VersionDelta {
	// TODO: Implement
	return []VersionDelta{{0, nil}}
}

// CompatibleSchemaSince determines if there was incompatible schema change since given version.
func (d *DefaultCollection) CompatibleSchemaSince(version int32) bool {
	// first element in the array contains last incompatible schema change
	return version >= d.SchemaDeltas[0].version
}

// UpdateRowSchema fixes the schema of  provided, unmarshalled, document from given version to the latest
// collection schema.
func (d *DefaultCollection) UpdateRowSchema(_ map[string]any, version int32) error {
	if d.CompatibleSchemaSince(version) {
		return nil
	}

	// TODO: Implement
	return nil
}

// UpdateRowSchemaRaw fixes the schema of provided document from given version to the latest
// collection schema.
func (d *DefaultCollection) UpdateRowSchemaRaw(doc []byte, version int32) ([]byte, error) {
	if d.CompatibleSchemaSince(version) {
		return doc, nil
	}

	decDoc, err := ljson.Decode(doc)
	if ulog.E(err) {
		return nil, err
	}

	if err = d.UpdateRowSchema(decDoc, version); err != nil {
		return nil, err
	}

	if doc, err = ljson.Encode(decDoc); err != nil {
		return nil, err
	}

	return doc, nil
}
