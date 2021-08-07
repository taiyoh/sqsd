package memorylocker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taiyoh/sqsd/locker"
)

func TestMemoryLocker(t *testing.T) {
	l := NewMemoryQueueLocker()
	ctx := context.Background()

	assert.NoError(t, l.Lock(ctx, "hogefuga"))
	assert.NoError(t, l.Lock(ctx, "foobarbaz"))
	assert.ErrorIs(t, l.Lock(ctx, "foobarbaz"), locker.ErrQueueExists)

	ids, err := l.Find(ctx, time.Now())
	assert.NoError(t, err)
	assert.Equal(t, []string{"foobarbaz", "hogefuga"}, ids)

	ids, err = l.Find(ctx, time.Now().Add(expireDur))
	assert.NoError(t, err)
	assert.Empty(t, ids)

	assert.NoError(t, l.Unlock(ctx, "aiueo"))
	ids, err = l.Find(ctx, time.Now())
	assert.NoError(t, err)
	assert.Equal(t, []string{"foobarbaz", "hogefuga"}, ids)
	assert.NoError(t, l.Unlock(ctx, "foobarbaz"))
	ids, err = l.Find(ctx, time.Now())
	assert.NoError(t, err)
	assert.Equal(t, []string{"hogefuga"}, ids)
	assert.NoError(t, l.Unlock(ctx, "hogefuga"))
	ids, err = l.Find(ctx, time.Now())
	assert.NoError(t, err)
	assert.Empty(t, ids)
}

func TestRunQueueLocker(t *testing.T) {
	l := NewMemoryQueueLocker(Duration(50 * time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunQueueLocker(ctx, l, 30*time.Millisecond)

	assert.NoError(t, l.Lock(ctx, "hooooooooo"))
	time.Sleep(25 * time.Millisecond)
	assert.NoError(t, l.Lock(ctx, "baaaaaaaaa"))

	for _, refs := range [][]string{
		{
			"baaaaaaaaa", "hooooooooo",
		},
		{},
	} {
		ids, _ := l.Find(ctx, time.Now().UTC())
		assert.Equal(t, refs, ids)
		time.Sleep(30 * time.Millisecond)
	}
}
