package utils

import (
	"time"

	"github.com/cockroachdb/errors"
)

// ParseTime returns time.Time from text represented time
func ParseTime(t string) (time.Time, error) {
	tout, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return tout, errors.Wrapf(err, "failed to parse time %q to time.Time", t)
	}
	return tout, nil
}

// MakeTimeToString returns text represented time from time.Time
func MakeTimeToString(t time.Time) string {
	return t.Format(time.RFC3339)
}
