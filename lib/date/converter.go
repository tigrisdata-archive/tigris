package date

import (
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
)

const (
	// RFC3339Nano time layout
	RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
)

// ToUnixNano converts a RFC3339 to Unix nano seconds
func ToUnixNano(dateStr string) (int64, error) {
	t, err := time.Parse(RFC3339Nano, dateStr)
	if err != nil {
		return 0, api.Errorf(api.Code_INVALID_ARGUMENT, "Validation failed, %s is not a valid date-time", dateStr)
	}
	return t.UnixNano(), nil
}
