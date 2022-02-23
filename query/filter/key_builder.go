package filter

import (
	"github.com/tigrisdata/tigrisdb/keys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KeyBuilder is responsible for building internal Keys. A composer is caller by the builder to build the internal keys
// based on the Composer logic.
type KeyBuilder struct {
	composer KeyComposer
}

// NewKeyBuilder returns a KeyBuilder
func NewKeyBuilder(composer KeyComposer) *KeyBuilder {
	return &KeyBuilder{
		composer: composer,
	}
}

// Build is responsible for building the internal keys from the user filter and using the keys defined in the schema
// and passed by the caller in this method. The build is doing a level by level traversal to build the internal Keys.
// On each level multiple keys can be formed because the user can specify ranges. The builder is not deciding the logic
// of key generation, the builder is simply traversing on the filters and calling compose where the logic resides.
func (k *KeyBuilder) Build(filters []Filter, userDefinedKeys []string) ([]keys.Key, error) {
	var queue []Filter
	var singleLevel []*Selector
	var allKeys []keys.Key
	for _, f := range filters {
		if ss, ok := f.(*Selector); ok {
			singleLevel = append(singleLevel, ss)
		} else {
			queue = append(queue, f)
		}
	}
	if len(singleLevel) > 0 {
		// if we have something on top level
		iKeys, err := k.composer.Compose(singleLevel, userDefinedKeys, AndOP)
		if err != nil {
			return nil, err
		}
		allKeys = append(allKeys, iKeys...)
	}

	for len(queue) > 0 {
		element := queue[0]
		if e, ok := element.(LogicalFilter); ok {
			var singleLevel []*Selector
			for _, ee := range e.GetFilters() {
				if ss, ok := ee.(*Selector); ok {
					singleLevel = append(singleLevel, ss)
				} else {
					queue = append(queue, ee)
				}
			}

			iKeys, err := k.composer.Compose(singleLevel, userDefinedKeys, e.Type())
			if err != nil {
				return nil, err
			}
			allKeys = append(allKeys, iKeys...)
		}
		queue = queue[1:]
	}

	return allKeys, nil
}

// KeyComposer needs to be implemented to have a custom Compose method with different constraints.
type KeyComposer interface {
	Compose(level []*Selector, userDefinedKeys []string, parent LogicalOP) ([]keys.Key, error)
}

// StrictEqKeyComposer is to generate internal keys only if the condition is equality on the fields that are part of
// the schema and all these fields are present in the filters. The following rules are applied for StrictEqKeyComposer
//  - The userDefinedKeys(indexes defined in the schema) passed in parameter should be present in the filter
//  - For AND filters it is possible to build internal keys for composite indexes, for OR it is not possible.
// So for OR filter an error is returned if it is used for indexes that are composite.
type StrictEqKeyComposer struct {
	Prefix string
}

func NewStrictEqKeyComposer(prefix string) *StrictEqKeyComposer {
	return &StrictEqKeyComposer{
		Prefix: prefix,
	}
}

// Compose is implementing the logic of composing keys
func (s *StrictEqKeyComposer) Compose(selectors []*Selector, userDefinedKeys []string, parent LogicalOP) ([]keys.Key, error) {
	var compositeKeys = make([][]*Selector, 1) // allocate just for the first keyParts
	for _, k := range userDefinedKeys {
		var repeatedFields []*Selector
		for _, sel := range selectors {
			if k == sel.Field {
				repeatedFields = append(repeatedFields, sel)
			}
			if sel.Matcher.Type() != EQ {
				return nil, status.Errorf(codes.InvalidArgument, "filters only supporting $eq comparison, found '%s'", sel.Matcher.Type())
			}
		}

		if len(repeatedFields) == 0 {
			// nothing found or a gap
			return nil, status.Errorf(codes.InvalidArgument, "filters doesn't contains primary key fields")
		}
		if len(repeatedFields) > 1 && parent == AndOP {
			// with AND there is no use of EQ on the same field
			return nil, status.Errorf(codes.InvalidArgument, "reusing same fields for conditions on equality")
		}

		compositeKeys[0] = append(compositeKeys[0], repeatedFields[0])
		// as we found some repeated fields in the filter so clone the first set of keys and add this prefix to all the
		// repeated fields, cloning is only needed if there are more than one repeated fields
		for j := 1; j < len(repeatedFields); j++ {
			keyPartsCopy := make([]*Selector, len(compositeKeys[0])-1)
			copy(keyPartsCopy, compositeKeys[0][0:len(compositeKeys[0])-1])
			keyPartsCopy = append(keyPartsCopy, repeatedFields[j])
			compositeKeys = append(compositeKeys, keyPartsCopy)
		}
	}

	// keys building is dependent on the filter type
	var allKeys []keys.Key
	for _, k := range compositeKeys {
		switch parent {
		case AndOP:
			var primaryKeyParts []interface{}
			for _, s := range k {
				primaryKeyParts = append(primaryKeyParts, s.Matcher.GetValue().Get())
			}

			allKeys = append(allKeys, keys.NewKey(s.Prefix, primaryKeyParts...))
		case OrOP:
			for _, sel := range k {
				if len(userDefinedKeys) > 1 {
					// this means OR can't build independently these keys
					return nil, status.Errorf(codes.InvalidArgument, "OR is not supported with composite primary keys")
				}

				allKeys = append(allKeys, keys.NewKey(s.Prefix, sel.Matcher.GetValue().Get()))
			}
		}
	}

	return allKeys, nil
}
