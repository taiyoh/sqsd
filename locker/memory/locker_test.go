package memorylocker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taiyoh/sqsd/v2/locker"
)

func TestMemoryLocker(t *testing.T) {
	l := New()
	ctx := context.Background()

	assert.NoError(t, l.Lock(ctx, "hogefuga"))
	assert.ErrorIs(t, l.Lock(ctx, "hogefuga"), locker.ErrQueueExists)

	t1 := time.Now().UTC()

	{
		ll := l.(*memoryLocker)
		ll.pool.Store("hogefuga", t1.Add(-2*time.Second))
		ll.pool.Store("foobarbaz", t1.Add(-1*time.Second))
	}

	for _, tt := range []struct {
		label string
		ts    time.Time
		want  []string
	}{
		{
			label: "case1",
			ts:    t1.Add(-3 * time.Second),
			want:  []string{"foobarbaz", "hogefuga"},
		},
		{
			label: "case2",
			ts:    t1.Add(-2*time.Second + 200*time.Millisecond),
			want:  []string{"foobarbaz"},
		},
		{
			label: "case3",
			ts:    t1,
			want:  []string{},
		},
	} {
		t.Run(tt.label, func(t *testing.T) {
			assert.NoError(t, l.Unlock(ctx, tt.ts))
			var keys []string
			l.(*memoryLocker).pool.Range(func(key, value interface{}) bool {
				keys = append(keys, key.(string))
				return true
			})
			assert.ElementsMatch(t, tt.want, keys)
		})
	}
}
