package memorylocker

import (
	"context"
	"sync"
	"time"

	"github.com/taiyoh/sqsd/locker"
)

type memoryLocker struct {
	pool sync.Map
	dur  time.Duration
}

// Option provides setting function to memory QueueLocker.
type Option func(*memoryLocker)

// Duration sets expire duration to memory QueueLocker.
func Duration(dur time.Duration) Option {
	return func(ml *memoryLocker) {
		ml.dur = dur
	}
}

// New creates QueueLocker to memory.
func New(opts ...Option) locker.QueueLocker {
	ml := &memoryLocker{
		dur: locker.DefaultExpireDuration,
	}
	for _, opt := range opts {
		opt(ml)
	}
	return ml
}

var _ locker.QueueLocker = (*memoryLocker)(nil)

func (l *memoryLocker) Lock(_ context.Context, queueID string) error {
	now := time.Now().UTC()
	if val, ok := l.pool.Load(queueID); ok && val.(time.Time).After(now) {
		return locker.ErrQueueExists
	}
	expire := now.Add(l.dur)
	l.pool.Store(queueID, expire)
	return nil
}

func (l *memoryLocker) Unlock(_ context.Context, ts time.Time) error {
	var keys []interface{}
	l.pool.Range(func(key, value interface{}) bool {
		if value.(time.Time).Before(ts) {
			keys = append(keys, key)
		}
		return true
	})
	for _, key := range keys {
		l.pool.Delete(key)
	}
	return nil
}
