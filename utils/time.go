package utils

import (
	"time"

	"golang.org/x/xerrors"
)

// ParseTime returns time.Time from text represented time
func ParseTime(t string) (time.Time, error) {
	tout, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return tout, xerrors.Errorf("failed to parse time '%s' to time.Time: %w", t, err)
	}
	return tout, nil
}

// MakeTimeToString returns text represented time from time.Time
func MakeTimeToString(t time.Time) string {
	return t.Format(time.RFC3339)
}
