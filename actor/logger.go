package sqsd

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
)

var logger = log.New(log.InfoLevel, "[sqsd]")

// SetLogLevel set supplied log.Level in actor, mailbox and our logger.
func SetLogLevel(l log.Level) {
	actor.SetLogLevel(l)
	mailbox.SetLogLevel(l)
	logger.SetLevel(l)
}
