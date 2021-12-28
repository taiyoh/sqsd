package locker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taiyoh/sqsd/locker"
	nooplocker "github.com/taiyoh/sqsd/locker/noop"
)

func TestUnlocker(t *testing.T) {
	l, err := locker.NewUnlocker(nil, 0)
	assert.Nil(t, l)
	assert.EqualError(t, err, "locker is required")
	nl := nooplocker.Get()

	l, err = locker.NewUnlocker(nl, 0)
	assert.Nil(t, l)
	assert.EqualError(t, err, "interval must be greater than 0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := make(chan time.Time, 5)
	nl.AddUnlockHook(func(c context.Context, t time.Time) error {
		stream <- t
		if len(stream) >= 3 {
			cancel()
		}
		return nil
	})

	l, err = locker.NewUnlocker(nl, 50*time.Microsecond, locker.ExpireDuration(time.Hour))
	assert.NoError(t, err)
	assert.NotNil(t, l)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		l.Run(ctx)
	}()
	wg.Wait()
	close(stream)
	var counter int
	now := time.Now().UTC()
	for tt := range stream {
		assert.True(t, tt.Before(now.Add(-time.Hour)))
		counter++
	}
	assert.Equal(t, 3, counter)
}
