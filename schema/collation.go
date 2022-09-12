package schema

import api "github.com/tigrisdata/tigris/api/server/v1"

const CollationKey string = "collation"

type CollationType uint8

const (
	Undefined CollationType = iota
	CaseInsensitive
	CaseSensitive
)

var SupportedCollations = [...]string{
	CaseInsensitive: "ci",
	CaseSensitive:   "cs",
}

func ToCollationType(userCollation string) CollationType {
	for ct, cs := range SupportedCollations {
		if cs == userCollation {
			return CollationType(ct)
		}
	}

	return Undefined
}

type Collation struct {
	Case string `json:"case,omitempty"`
}

func (c *Collation) IsCaseSensitive() bool {
	return c.Case == SupportedCollations[CaseSensitive]
}

func (c *Collation) IsCaseInsensitive() bool {
	return c.Case == SupportedCollations[CaseInsensitive]
}

func (c *Collation) IsValid() error {
	if caseType := ToCollationType(c.Case); caseType == Undefined {
		return api.Errorf(api.Code_INVALID_ARGUMENT, "collation %s is not supported", c.Case)
	}

	return nil
}
