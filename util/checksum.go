package util

import (
	"crypto/sha1"
	"encoding/hex"

	irods_util "github.com/cyverse/go-irodsclient/irods/util"
)

//TODO: remove these APIs not frequently used

// GetChecksumString returns string from checksum bytes
func GetChecksumString(checksum []byte) string {
	return hex.EncodeToString(checksum)
}

// GetSHA1Sum returns sha1 check sum string
func GetSHA1Sum(str string) (string, error) {
	hash, err := irods_util.HashStringsWithAlgorithm([]string{str}, sha1.New())
	if err != nil {
		return "", err
	}

	return GetChecksumString(hash), nil
}
