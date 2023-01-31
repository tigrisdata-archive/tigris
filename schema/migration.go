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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/util"
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
// If field is added then From is equal to `UnknownType`.
type VersionDeltaField struct {
	KeyPath []string  // Key path of the field
	From    FieldType // The type is changing from this
	To      FieldType // to this

	MaxLength int
}

// VersionDelta contains all fields schema changes in particular schema Version.
type VersionDelta struct {
	Version int
	Fields  []*VersionDeltaField
}

// arrayToMap converts fields array to map, for fast lookups by the field name.
func arrayToMap(schema []*Field) map[string]*Field {
	m := make(map[string]*Field)

	for _, v := range schema {
		m[v.FieldName] = v
	}

	return m
}

// buildSchemaDeltaLow is a recursive helper for building schema delta.
func buildSchemaDeltaLow(keyPath []string, first []*Field, second []*Field) []*VersionDeltaField {
	var fields []*VersionDeltaField

	sm := arrayToMap(second)

	for _, v1 := range first {
		// Intentionally appended to new slice, disabling gocritic lint
		kp := append(keyPath, v1.FieldName) //nolint:gocritic

		v2, ok := sm[v1.FieldName]
		switch {
		case !ok: // field deleted in new schema
			cp := make([]string, len(kp))
			copy(cp, kp)
			fields = append(fields, &VersionDeltaField{KeyPath: cp, From: v1.DataType, To: UnknownType})
		case v1.DataType != v2.DataType: // type changed
			cp := make([]string, len(kp))
			copy(cp, kp)
			var m int
			if v2.MaxLength != nil {
				m = int(*v2.MaxLength)
			}
			fields = append(fields, &VersionDeltaField{KeyPath: cp, From: v1.DataType, To: v2.DataType, MaxLength: m})
		case v2.MaxLength != nil && (v1.MaxLength == nil || *v1.MaxLength > *v2.MaxLength): // type changed
			cp := make([]string, len(kp))
			copy(cp, kp)
			fields = append(fields, &VersionDeltaField{KeyPath: cp, From: v1.DataType, To: v2.DataType, MaxLength: int(*v2.MaxLength)})
		case v1.DataType == ObjectType:
			fields = append(fields, buildSchemaDeltaLow(kp, v1.Fields, v2.Fields)...)
		case v1.DataType == ArrayType:
			fields = append(fields, buildSchemaDeltaLow(kp, v1.Fields, v2.Fields)...)
		}
	}

	return fields
}

// buildSchemaDelta build delta between two consecutive schema versions.
func buildSchemaDelta(schema1 Version, schema2 Version) (*VersionDelta, error) {
	var (
		first, second *Factory
		delta         VersionDelta
		err           error
	)

	if first, err = Build("", schema1.Schema); err != nil {
		return nil, err
	}

	if second, err = Build("", schema2.Schema); err != nil {
		return nil, err
	}

	delta.Version = schema2.Version
	delta.Fields = buildSchemaDeltaLow(nil, first.Fields, second.Fields)

	return &delta, nil
}

// buildSchemaDeltas builds all the schema deltas from the collection schema changes history.
func buildSchemaDeltas(schemas Versions) ([]VersionDelta, error) {
	var deltas []VersionDelta

	for i := 1; i < len(schemas); i++ {
		delta, err := buildSchemaDelta(schemas[i-1], schemas[i])
		if err != nil {
			return nil, err
		}

		// add only if there are changes
		if len(delta.Fields) > 0 {
			deltas = append(deltas, *delta)
		}
	}

	if len(deltas) != 0 {
		return deltas, nil
	}

	// Just one schema Version or no incompatible schema changes
	return []VersionDelta{{0, nil}}, nil
}

// FieldVersion contains individual field change,
// along with the schema version at which this change has happened.
type FieldVersion struct {
	Change  *VersionDeltaField
	Version int
}

// FieldVersions contains all the changes of the field,
// across all the schema versions.
type FieldVersions struct {
	versions []*FieldVersion

	// child is not nil for the object type.
	// empty string key means array type
	child map[string]*FieldVersions
}

// addFieldVersion adds individual field change to the tree.
func addFieldVersion(versions map[string]*FieldVersions, version int, change *VersionDeltaField) {
	m, i := versions, 0

	// traverse existing path
	for ; i < len(change.KeyPath); i++ {
		//		prev = m
		if m[change.KeyPath[i]] == nil {
			m[change.KeyPath[i]] = &FieldVersions{}
		}
		if i < len(change.KeyPath)-1 {
			if m[change.KeyPath[i]].child == nil {
				m[change.KeyPath[i]].child = make(map[string]*FieldVersions)
			}
			m = m[change.KeyPath[i]].child
		}
	}

	last := change.KeyPath[len(change.KeyPath)-1]
	m[last].versions = append(m[last].versions, &FieldVersion{Version: version, Change: change})
}

// buildFieldVersions build a tree of all incompatible field changes.
func buildFieldVersions(deltas []VersionDelta) map[string]*FieldVersions {
	fieldVersions := make(map[string]*FieldVersions)

	for _, d := range deltas {
		for _, f := range d.Fields {
			addFieldVersion(fieldVersions, d.Version, f)
		}
	}

	return fieldVersions
}

func lookupFieldVersion(fieldVersions map[string]*FieldVersions, keyPath []string) []*FieldVersion {
	m := fieldVersions
	i := 0

	// go down till the last element of keyPath and keyPath element exists in the tree
	for ; i < len(keyPath)-1 && m != nil && m[keyPath[i]] != nil; i++ {
		m = m[keyPath[i]].child
	}

	// keyPath doesn't exist in the tree
	if m == nil || m[keyPath[i]] == nil {
		return nil
	}

	// here 'i' is at last element, and it exists in the tree
	return m[keyPath[i]].versions
}

// LookupFieldVersion returns the list of the schema versions for the specified keyPath,
// when this field had incompatible change.
// Returns nil if the field has never been changed, meaning that it has version 1.
func (d *DefaultCollection) LookupFieldVersion(keyPath []string) []*FieldVersion {
	return lookupFieldVersion(d.FieldVersions, keyPath)
}

// CompatibleSchemaSince determines if there was incompatible schema change since given version.
func (d *DefaultCollection) CompatibleSchemaSince(version int32) bool {
	// last element in the array contains last incompatible schema change
	return int(version) >= d.SchemaDeltas[len(d.SchemaDeltas)-1].Version
}

func convertFromBool(toType FieldType, val bool) any {
	switch toType {
	case BoolType:
		return val
	case Int32Type, Int64Type, DoubleType:
		if val {
			return 1
		}
		return 0
	case StringType:
		if val {
			return "true"
		}
		return "false"
	case ByteType:
		if val {
			return []byte{0x1}
		}
		return []byte{0x0}
	case UUIDType, DateTimeType, ArrayType, ObjectType:
		// non convertible
	}

	return nil
}

func convertFromBytes(toType FieldType, val []byte, maxLength int) any {
	switch toType {
	case StringType:
		if maxLength > 0 && len(val) > maxLength {
			return string(val[:maxLength])
		}

		return string(val)
	case ByteType:
		if maxLength > 0 && len(val) > maxLength {
			return val[:maxLength]
		}

		return val
	case UUIDType:
		if len(val) == 16 {
			u, err := uuid.FromBytes(val)
			if err == nil {
				return u.String()
			}
		}
	case Int64Type:
		if len(val) == 8 {
			return int64(binary.BigEndian.Uint64(val))
		}
	case Int32Type:
		if len(val) == 4 {
			return int64(binary.BigEndian.Uint32(val))
		}
	case BoolType:
		if len(val) == 1 {
			return val[0] != 0x0
		}
	case DoubleType: // consider input bytes as binary representation of float64
		if len(val) == 8 {
			bits := binary.BigEndian.Uint64(val)
			return math.Float64frombits(bits)
		}
	case DateTimeType: // consider input bytes as binary representation of Unix time-stamp
		if len(val) == 8 {
			return time.Unix(0, int64(binary.BigEndian.Uint64(val))).UTC().Format(time.RFC3339Nano)
		}
	case ArrayType, ObjectType:
		// TODO: We may consider bytes as raw object representation and unmarshal it
		// TODO: Schema wouldn't be enforced though
	}
	return nil
}

func convertFromInt64(toType FieldType, val int64) any {
	switch toType {
	case BoolType:
		return val != 0
	case Int32Type:
		if val < math.MaxInt32 && val > math.MinInt32 {
			return val
		}
	case Int64Type:
		return val
	case DoubleType:
		return float64(val) // FIXME: Only ~53 bits convertible
	case StringType:
		return strconv.FormatInt(val, 10)
	case ByteType:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(val))
		return b
	case UUIDType, DateTimeType, ArrayType, ObjectType:
		// non convertible
	}
	return nil
}

func convertFromDouble(toType FieldType, val float64) any {
	switch toType {
	case BoolType:
		return val != 0
	case Int32Type:
		f := int64(math.Floor(val))
		if f < math.MaxInt32 && f > math.MinInt32 {
			return f
		}
	case Int64Type:
		return int64(math.Floor(val))
	case DoubleType:
		return val
	case StringType:
		return strconv.FormatFloat(val, 'g', 10, 64)
	case ByteType:
		b := make([]byte, 8)
		bits := math.Float64bits(val)
		binary.BigEndian.PutUint64(b, bits)
		return b
	case UUIDType, DateTimeType, ArrayType, ObjectType:
		// non convertible
	}
	return nil
}

func convertFromString(toType FieldType, val string, maxLength int) any {
	switch toType {
	case BoolType:
		if val == "true" {
			return true
		}
		if val == "false" {
			return false
		}
	case Int32Type:
		if i, err := strconv.ParseInt(val, 10, 32); err == nil {
			return i
		}
	case Int64Type:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	case DoubleType:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	case StringType:
		if maxLength > 0 && len(val) > maxLength {
			return val[:maxLength]
		}
		return val
	case ByteType:
		if maxLength > 0 && len(val) > maxLength {
			return []byte(val[:maxLength])
		}
		return []byte(val)
	case UUIDType:
		if _, err := uuid.Parse(val); err == nil {
			return val
		}
	case DateTimeType:
		// TODO: Detect more date time formats
		if _, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return val
		}
	case ArrayType, ObjectType:
		// non convertible
	}
	return nil
}

func convertType(change *VersionDeltaField, val any) any {
	switch v := val.(type) {
	case bool:
		return convertFromBool(change.To, v)
	case string:
		if change.From == ByteType {
			if b, err := base64.StdEncoding.DecodeString(v); err == nil {
				return convertFromBytes(change.To, b, change.MaxLength)
			}
		} else {
			return convertFromString(change.To, v, change.MaxLength)
		}
	case json.Number: // comes from initial document unmarshal
		switch change.From {
		case Int64Type, Int32Type:
			if i, err := v.Int64(); err == nil {
				return convertFromInt64(change.To, i)
			}
		case DoubleType:
			if i, err := v.Float64(); err == nil {
				return convertFromDouble(change.To, i)
			}
		}
	case float64: // float64, int64 comes from our conversion
		return convertFromDouble(change.To, v)
	case int64:
		return convertFromInt64(change.To, v)
	}

	return nil
}

// applyArrayFieldDelta applies delta to every array element,
// recursively descending multidimensional arrays.
// If array element is not convertible to target type function returns false,
// so the caller can remove the array entirely.
func applyArrayFieldDelta(arr []any, change *VersionDeltaField, i int) bool {
	for key, v := range arr {
		switch m := v.(type) {
		case map[string]any:
			if !applyObjectFieldDelta(m, change, i+1) {
				delete(m, change.KeyPath[i])
				return false
			}
		case []any:
			if !applyArrayFieldDelta(m, change, i+1) {
				return false
			}
		default:
			arr[key] = convertType(change, m)
			if arr[key] == nil {
				return false
			}
		}
	}

	return true
}

func applyPrimitiveFieldDelta(doc map[string]any, change *VersionDeltaField) bool {
	key := change.KeyPath[len(change.KeyPath)-1]

	// deleted field
	if change.To == UnknownType {
		delete(doc, key)
		return true
	}

	v := convertType(change, doc[key])
	if v == nil {
		delete(doc, key)
		return false
	} else {
		doc[key] = v
		return true
	}
}

func applyObjectFieldDelta(doc map[string]any, change *VersionDeltaField, start int) bool {
	// descend to the last object in the key path
	for i := start; i < len(change.KeyPath)-1; i++ {
		switch m := doc[change.KeyPath[i]].(type) {
		case map[string]any:
			doc = m
		case []any:
			if !applyArrayFieldDelta(m, change, i+1) {
				delete(doc, change.KeyPath[i])
				return false
			}
			return true
		default:
			return false
		}
	}

	return applyPrimitiveFieldDelta(doc, change)
}

func applySchemaDelta(doc map[string]any, delta *VersionDelta) {
	for _, d := range delta.Fields {
		applyObjectFieldDelta(doc, d, 0)
	}
}

// UpdateRowSchema fixes the schema of provided, unmarshalled, document from given Version to the latest
// collection schema.
func (d *DefaultCollection) UpdateRowSchema(doc map[string]any, version int32) {
	// Find first incompatible schema after the row schema version
	i := sort.Search(len(d.SchemaDeltas), func(i int) bool { return d.SchemaDeltas[i].Version > int(version) })

	// Apply all deltas after first incompatible change
	for ; i < len(d.SchemaDeltas); i++ {
		log.Debug().Int("to_version", d.SchemaDeltas[i].Version).Interface("doc", doc).Msg("Updating row schema")
		applySchemaDelta(doc, &d.SchemaDeltas[i])
	}
}

// UpdateRowSchemaRaw fixes the schema of provided document from given Version to the latest
// collection schema.
func (d *DefaultCollection) UpdateRowSchemaRaw(doc []byte, version int32) ([]byte, error) {
	decDoc, err := util.JSONToMap(doc)
	if ulog.E(err) {
		return nil, err
	}

	d.UpdateRowSchema(decDoc, version)

	if doc, err = util.MapToJSON(decDoc); err != nil {
		return nil, err
	}

	return doc, nil
}
