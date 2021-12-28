package memorylocker

import (
	"context"
	"sync"
	"time"

	"github.com/taiyoh/sqsd/locker"
)

type memoryLocker struct {
	pool sync.Map
}

// New creates QueueLocker by memory.
func New() locker.QueueLocker {
	return &memoryLocker{}
}

var _ locker.QueueLocker = (*memoryLocker)(nil)

func (l *memoryLocker) Lock(_ context.Context, queueID string) error {
	now := time.Now().UTC()
	if _, ok := l.pool.Load(queueID); ok {
		return locker.ErrQueueExists
	}
	l.pool.Store(queueID, now)
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
