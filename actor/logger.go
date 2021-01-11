package sqsd

import (
	"encoding"
	"fmt"
	"strings"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
)

// LogLevel sets log level for sqsd.
type LogLevel struct {
	Level log.Level
}

var _ encoding.TextUnmarshaler = (*LogLevel)(nil)

var levelMap = map[string]log.Level{
	"debug": log.DebugLevel,
	"info":  log.InfoLevel,
	"warn":  log.WarnLevel,
	"error": log.ErrorLevel,
}

// UnmarshalText scans levelMap from supplied text and set scanned level to LogLevel.
func (l *LogLevel) UnmarshalText(text []byte) error {
	lv, ok := levelMap[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("invalid log_level: %s", text)
	}
	l.Level = lv
	return nil
}

var logger = log.New(log.InfoLevel, "[sqsd]")

// SetLogLevel set supplied log.Level in actor, mailbox and our logger.
func SetLogLevel(ll LogLevel) {
	l := ll.Level
	actor.SetLogLevel(l)
	mailbox.SetLogLevel(l)
	logger.SetLevel(l)
}
