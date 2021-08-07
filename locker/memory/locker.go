package memorylocker

import (
	"context"
	"sort"
	"strings"
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

var expireDur = 24 * time.Hour

// Duration sets expire duration to memory QueueLocker.
func Duration(dur time.Duration) Option {
	return func(ml *memoryLocker) {
		ml.dur = dur
	}
}

// New creates QueueLocker to memory.
func New(opts ...Option) locker.QueueLocker {
	ml := &memoryLocker{
		dur: expireDur,
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

func (l *memoryLocker) Find(_ context.Context, ts time.Time) ([]string, error) {
	ids := make([]string, 0, 8)
	l.pool.Range(func(key, value interface{}) bool {
		if value.(time.Time).After(ts) {
			ids = append(ids, key.(string))
		}
		return true
	})
	sort.Slice(ids, func(i, j int) bool {
		return strings.Compare(ids[i], ids[j]) < 0
	})
	return ids, nil
}

func (l *memoryLocker) Unlock(_ context.Context, ids ...string) error {
	for _, id := range ids {
		l.pool.Delete(id)
	}
	return nil
}
