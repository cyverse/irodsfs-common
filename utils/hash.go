package utils

import (
	"crypto/sha1"
	"encoding/hex"
)

// MakeHash returns hash string from plain text
func MakeHash(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes)
}
