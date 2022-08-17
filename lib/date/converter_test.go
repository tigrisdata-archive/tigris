package date

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToUnixNano(t *testing.T) {
	validCases := []struct {
		name     string
		date     string
		expected int64
	}{
		{"UTC RFC 3339", "2022-10-18T00:51:07+00:00", 1666054267000000000},
		{"UTC RFC 3339 Nano", "2022-10-18T00:51:07.528106+00:00", 1666054267528106000},
		{"IST RFC 3339", "2022-10-11T04:19:32+05:30", 1665442172000000000},
		{"IST RFC 3339 Nano", "2022-10-18T00:51:07.999999999+05:30", 1666034467999999999},
		{"No TZ RFC 3339", "2022-10-18T00:51:07Z", 1666054267000000000},
	}

	for _, v := range validCases {
		t.Run(v.name, func(t *testing.T) {
			actual, err := ToUnixNano(v.date)
			assert.NoError(t, err)
			assert.Equal(t, v.expected, actual)
		})
	}

	failureCases := []struct {
		name      string
		date      string
		errorLike string
	}{
		{"RFC 1123", "Mon, 02 Jan 2006 15:04:05 MST", "Validation failed"},
	}

	for _, v := range failureCases {
		t.Run(v.name, func(t *testing.T) {
			_, err := ToUnixNano(v.date)
			assert.ErrorContains(t, err, v.errorLike)
		})
	}
}
