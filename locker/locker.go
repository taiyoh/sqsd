package locker

import (
	"context"
	"errors"
	"time"
)

// QueueLocker represents locker interface for suppressing queue duplication.
type QueueLocker interface {
	Lock(ctx context.Context, key string) error
	Unlock(ctx context.Context, before time.Time) error
}

// DefaultExpireDuration shows that duration of remaining queue_id in locker is 24 hours.
var DefaultExpireDuration = 24 * time.Hour

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
			if err := l.Unlock(ctx, time.Now().UTC().Add(DefaultExpireDuration)); err != nil {
				// TODO: logging
				continue
			}
		}
	}
}
