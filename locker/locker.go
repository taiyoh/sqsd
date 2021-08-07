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

// RunQueueLocker scan deletable ids and delete from QueueLocker periodically.
func RunQueueLocker(ctx context.Context, l QueueLocker, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			ids, err := l.Find(ctx, time.Now().UTC())
			if err != nil {
				// TODO: logging
				continue
			}
			if err := l.Unlock(ctx, ids...); err != nil {
				// TODO: logging
				continue
			}
		}
	}
}
