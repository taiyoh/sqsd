package nooplocker

import (
	"context"
	"time"

	"github.com/taiyoh/sqsd/v2/locker"
)

type noopLocker struct {
	lockHooks   []func(context.Context, string) error
	unlockHooks []func(context.Context, time.Time) error
}

// LockerWithHooks has noopLocker instance which has hooks interface.
type LockerWithHooks interface {
	locker.QueueLocker
	AddLockHook(f func(context.Context, string) error)
	AddUnlockHook(f func(context.Context, time.Time) error)
}

// Get returns new noopLocker instance.
func Get() LockerWithHooks {
	return &noopLocker{}
}

func (l *noopLocker) Lock(ctx context.Context, key string) error {
	for _, f := range l.lockHooks {
		_ = f(ctx, key)
	}
	return nil
}

func (l *noopLocker) Unlock(ctx context.Context, before time.Time) error {
	for _, f := range l.unlockHooks {
		_ = f(ctx, before)
	}
	return nil
}

func (l *noopLocker) AddLockHook(f func(context.Context, string) error) {
	l.lockHooks = append(l.lockHooks, f)
}

func (l *noopLocker) AddUnlockHook(f func(context.Context, time.Time) error) {
	l.unlockHooks = append(l.unlockHooks, f)
}
