package util

import (
	"time"

	"github.com/cockroachdb/errors"
)

// ParseTime parses RFC3339 formatted time string to time.Time
func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return t, errors.Wrapf(err, "failed to parse time %q", s)
	}
	return t, nil
}

// TimeString formats time.Time as RFC3339 string
func TimeString(t time.Time) string {
	return t.Format(time.RFC3339)
}
