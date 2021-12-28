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

const defaultExpireDuration = 24 * time.Hour

// ErrQueueExists shows this queue is already registered.
var ErrQueueExists = errors.New("queue exists")

// Unlocker removes unused queue_id list periodically.
type Unlocker struct {
	interval time.Duration
	expire   time.Duration
	locker   QueueLocker
}

// UnlockerOption is an option for Unlocker.
type UnlockerOption func(*Unlocker)

// ExpireDuration sets expire duration to Unlocker.
func ExpireDuration(dur time.Duration) UnlockerOption {
	return func(u *Unlocker) {
		u.expire = dur
	}
}

// NewUnlocker creates Unlocker.
// As default, expire duration is 24 hours.
func NewUnlocker(l QueueLocker, interval time.Duration, opts ...UnlockerOption) (*Unlocker, error) {
	if l == nil {
		return nil, errors.New("locker is required")
	}
	if interval <= 0 {
		return nil, errors.New("interval must be greater than 0")
	}
	ul := &Unlocker{
		interval: interval,
		expire:   defaultExpireDuration,
		locker:   l,
	}
	for _, opt := range opts {
		opt(ul)
	}
	return ul, nil
}

// Run runs unlock operation by interval.
func (u Unlocker) Run(ctx context.Context) {
	tick := time.NewTicker(u.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := u.locker.Unlock(ctx, time.Now().UTC().Add(-u.expire)); err != nil {
				// TODO: logging
				continue
			}
		}
	}
}
