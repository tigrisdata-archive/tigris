package date

import (
	"time"

	"github.com/tigrisdata/tigris/schema"
)

// ToUnixNano converts a RFC3339 to Unix nano seconds
func ToUnixNano(dateStr string) (int64, error) {
	t, err := time.Parse(schema.DateTimeFormat, dateStr)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}
