package sqsd

import (
	"os"

	"github.com/AsynkronIT/protoactor-go/log"
)

var logger *log.Logger

func init() {
	levelMap := map[string]log.Level{
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"error": log.ErrorLevel,
	}
	l := log.InfoLevel
	if ll, ok := os.LookupEnv("LOG_LEVEL"); ok {
		lll, ok := levelMap[ll]
		if !ok {
			panic("invalid LOG_LEVEL")
		}
		l = lll
	}
	logger = log.New(l, "")
}
