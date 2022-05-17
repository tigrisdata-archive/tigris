package encoding

const (
	reservedSubspaceName = "reserved"
	encodingSubspaceName = "encoding"
	schemaSubspaceName   = "schema"
)

// MDNameRegistry provides the names of the internal tables(subspaces) maintained by the metadata package. The interface
// helps in creating test tables for these structures.
type MDNameRegistry interface {
	// ReservedSubspaceName is the name of the table(subspace) where all the counters are stored.
	ReservedSubspaceName() []byte

	// EncodingSubspaceName is the name of the table(subspace) which is used by the dictionary encoder to store all the
	// dictionary encoded values.
	EncodingSubspaceName() []byte

	// SchemaSubspaceName (the schema subspace) will be storing the actual schema of the user for a collection. The schema subspace will
	// look like below
	//    ["schema", 0x01, x, 0x01, 0x03, "created", 0x01] => {"title": "t1", properties: {"a": int}, primary_key: ["a"]}
	//
	//  where,
	//    - schema is the keyword for this table.
	//    - 0x01 is the schema subspace version
	//    - x is the value assigned for the namespace
	//    - 0x01 is the value for the database.
	//    - 0x03 is the value for the collection.
	//    - "created" is keyword.
	//
	SchemaSubspaceName() []byte
}

// DefaultMDNameRegistry provides the names of the subspaces used by the metadata package for managing dictionary
// encoded values, counters and schemas.
type DefaultMDNameRegistry struct{}

func (d *DefaultMDNameRegistry) ReservedSubspaceName() []byte {
	return []byte(reservedSubspaceName)
}

func (d *DefaultMDNameRegistry) EncodingSubspaceName() []byte {
	return []byte(encodingSubspaceName)
}

func (d *DefaultMDNameRegistry) SchemaSubspaceName() []byte {
	return []byte(schemaSubspaceName)
}

// TestMDNameRegistry is used by tests to inject table names that can be used by tests
type TestMDNameRegistry struct {
	ReserveSB  string
	EncodingSB string
	SchemaSB   string
}

func (d *TestMDNameRegistry) ReservedSubspaceName() []byte {
	return []byte(d.ReserveSB)
}

func (d *TestMDNameRegistry) EncodingSubspaceName() []byte {
	return []byte(d.EncodingSB)
}

func (d *TestMDNameRegistry) SchemaSubspaceName() []byte {
	return []byte(d.SchemaSB)
}
