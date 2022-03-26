package util

import (
	"runtime/debug"

	log "github.com/sirupsen/logrus"
)

func StackTraceFromPanic(logger *log.Entry) {
	if r := recover(); r != nil {
		logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
		logger.Panic(r)
	}
}
