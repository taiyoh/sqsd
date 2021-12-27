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

	t.Run("no items", func(t *testing.T) {
		ids, err := obj.Find(ctx, t0)
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})

	t.Run("one item", func(t *testing.T) {
		ids, err := obj.Find(ctx, t1)
		assert.NoError(t, err)
		assert.Equal(t, []string{"q1"}, ids)
	})

	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, obj.Lock(ctx, "q2"))

	t.Run("q2 not found", func(t *testing.T) {
		ids, err := obj.Find(ctx, t1)
		assert.NoError(t, err)
		assert.Equal(t, []string{"q1"}, ids)
	})

	t.Run("duplicate q1", func(t *testing.T) {
		err := obj.Lock(ctx, "q1")
		assert.ErrorIs(t, err, locker.ErrQueueExists)
	})

	t.Run("q1 and q2 found", func(t *testing.T) {
		ids, err := obj.Find(ctx, time.Now())
		assert.NoError(t, err)
		assert.Equal(t, []string{"q1", "q2"}, ids)
	})

	t.Run("remove q1", func(t *testing.T) {
		assert.NoError(t, obj.Unlock(ctx, "q1"))
		ids, err := obj.Find(ctx, time.Now())
		assert.NoError(t, err)
		assert.Equal(t, []string{"q2"}, ids)
	})
}
