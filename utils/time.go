package utils

import "time"

// ParseTime returns time.Time from text represented time
func ParseTime(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)
}

// MakeTimeToString returns text represented time from time.Time
func MakeTimeToString(t time.Time) string {
	return t.Format(time.RFC3339)
}
