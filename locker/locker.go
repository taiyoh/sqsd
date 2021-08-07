package locker

import (
	"context"
	"errors"
	"time"
)

// QueueLocker represents locker interface for suppressing queue duplication.
type QueueLocker interface {
	Lock(context.Context, string) error
	Find(context.Context, time.Time) ([]string, error)
	Unlock(context.Context, ...string) error
}

// ErrQueueExists shows this queue is already registered.
var ErrQueueExists = errors.New("queue exists")
