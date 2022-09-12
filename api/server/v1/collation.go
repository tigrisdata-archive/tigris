package api

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

func (x *Collation) IsCaseSensitive() bool {
	return x.Case == SupportedCollations[CaseSensitive]
}

func (x *Collation) IsCaseInsensitive() bool {
	return x.Case == SupportedCollations[CaseInsensitive]
}

func (x *Collation) IsValid() error {
	if caseType := ToCollationType(x.Case); caseType == Undefined {
		return Errorf(Code_INVALID_ARGUMENT, "collation '%s' is not supported", x.Case)
	}

	return nil
}

func ToCollationType(userCollation string) CollationType {
	for ct, cs := range SupportedCollations {
		if cs == userCollation {
			return CollationType(ct)
		}
	}

	return Undefined
}
