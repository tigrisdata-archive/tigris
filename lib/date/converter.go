package date

import (
	"time"
)

// ToUnixNano converts a string formatted time to Unix nanoseconds
func ToUnixNano(format string, dateStr string) (int64, error) {
	t, err := time.Parse(format, dateStr)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}
