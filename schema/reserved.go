package schema

type ReservedField uint8

const (
	CreatedAt ReservedField = iota
	UpdatedAt
	Metadata
	IdToSearchKey
)

var ReservedFields = [...]string{
	CreatedAt:     "created_at",
	UpdatedAt:     "updated_at",
	Metadata:      "metadata",
	IdToSearchKey: "_tigris_id",
}

func IsReservedField(name string) bool {
	for _, r := range ReservedFields {
		if r == name {
			return true
		}
	}

	return false
}
