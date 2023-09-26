package redislocker

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/taiyoh/sqsd/v2/locker"
)

func TestRedsislocker(t *testing.T) {
	db := rand.Intn(16)
	cli, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6379"},
		SelectDB:    db,
	})
	assert.NoError(t, err)
	t.Cleanup(func() {
		cli.Do(context.Background(), cli.B().Flushdb().Build())
		cli.Close()
	})
	// cli.AddHook(&traceHook{})
	obj := New(cli, "testKey")

	t0 := time.Now() // before lock
	ctx := context.Background()

	assert.NoError(t, obj.Lock(ctx, "q1"))
	time.Sleep(10 * time.Millisecond)
	t1 := time.Now() // after q1 lock

	t.Run("no items removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t0))
		result := cli.Do(ctx, cli.B().Zrangebyscore().Key("testKey").Min("-inf").Max("+inf").Build())
		assert.NoError(t, result.Error())
		ids, err := result.AsStrSlice()
		assert.NoError(t, err)
		assert.Equal(t, []string{"q1"}, ids)
	})

	t.Run("duplicate q1", func(t *testing.T) {
		err := obj.Lock(ctx, "q1")
		assert.ErrorIs(t, err, locker.ErrQueueExists)
	})

	t.Run("item removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t1))
		result := cli.Do(ctx, cli.B().Zrangebyscore().Key("testKey").Min("-inf").Max("+inf").Build())
		assert.NoError(t, result.Error())
		ids, err := result.AsStrSlice()
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})

	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, obj.Lock(ctx, "q2"))

	t.Run("q2 not removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t1))
		result := cli.Do(ctx, cli.B().Zrangebyscore().Key("testKey").Min("-inf").Max("+inf").Build())
		assert.NoError(t, result.Error())
		ids, err := result.AsStrSlice()
		assert.NoError(t, err)
		assert.Equal(t, []string{"q2"}, ids)
	})

	t.Run("q2 removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, time.Now()))
		result := cli.Do(ctx, cli.B().Zrangebyscore().Key("testKey").Min("-inf").Max("+inf").Build())
		assert.NoError(t, result.Error())
		ids, err := result.AsStrSlice()
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})
}
