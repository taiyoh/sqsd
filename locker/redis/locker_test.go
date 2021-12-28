package redislocker

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/taiyoh/sqsd/locker"
)

type traceHook struct {
	redis.Hook
}

func (traceHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	log.Printf("[REDIS] %s\n", cmd.String())
	return ctx, nil
}

func (traceHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	return nil
}

func (traceHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (traceHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}

func TestRedsislocker(t *testing.T) {
	db := rand.Intn(16)
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   db,
	})
	t.Cleanup(func() {
		cli.FlushDB(context.Background())
		cli.Close()
	})
	cli.AddHook(&traceHook{})
	obj := New(cli, "testKey")

	t0 := time.Now() // before lock
	ctx := context.Background()

	assert.NoError(t, obj.Lock(ctx, "q1"))
	time.Sleep(10 * time.Millisecond)
	t1 := time.Now() // after q1 lock

	t.Run("no items removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t0))
		ids, err := cli.ZRangeByScore(ctx, "testKey", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
		assert.NoError(t, err)
		assert.Equal(t, []string{"q1"}, ids)
	})

	t.Run("duplicate q1", func(t *testing.T) {
		err := obj.Lock(ctx, "q1")
		assert.ErrorIs(t, err, locker.ErrQueueExists)
	})

	t.Run("item removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t1))
		ids, err := cli.ZRangeByScore(ctx, "testKey", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})

	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, obj.Lock(ctx, "q2"))

	t.Run("q2 not removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, t1))
		ids, err := cli.ZRangeByScore(ctx, "testKey", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
		assert.NoError(t, err)
		assert.Equal(t, []string{"q2"}, ids)
	})

	t.Run("q2 removed", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, time.Now()))
		ids, err := cli.ZRangeByScore(ctx, "testKey", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})
}
