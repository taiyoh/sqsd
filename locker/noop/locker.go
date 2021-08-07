package nooplocker

import (
	"context"
	"time"

	"github.com/taiyoh/sqsd/locker"
)

type noopLocker struct{}

// Instance has noopLocker instance.
var Instance locker.QueueLocker = &noopLocker{}

func (noopLocker) Lock(_ context.Context, _ string) error {
	return nil
}

func (noopLocker) Find(_ context.Context, _ time.Time) ([]string, error) {
	return nil, nil
}

func (noopLocker) Unlock(_ context.Context, _ ...string) error {
	return nil
}
