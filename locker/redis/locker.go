package redislocker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/taiyoh/sqsd/locker"
)

type redislocker struct {
	keyName string
	cli     *redis.Client
}

var _ locker.QueueLocker = (*redislocker)(nil)

// New creates QueueLocker by Redis.
func New(cli *redis.Client, keyName string) locker.QueueLocker {
	return &redislocker{
		keyName: keyName,
		cli:     cli,
	}
}

func (l *redislocker) Lock(ctx context.Context, queueID string) error {
	now := time.Now().UTC()
	result, err := l.cli.ZAddNX(ctx, l.keyName, &redis.Z{
		Score:  float64(now.UnixNano()),
		Member: queueID,
	}).Result()

	switch {
	case err != nil:
		return err
	case result == 0:
		return locker.ErrQueueExists
	default:
		return nil
	}
}

func (l *redislocker) Find(ctx context.Context, ts time.Time) ([]string, error) {
	// ascending order
	return l.cli.ZRangeByScore(ctx, l.keyName, &redis.ZRangeBy{
		Max: fmt.Sprintf("(%d", ts.UnixNano()),
		Min: "-inf",
	}).Result()
}

func (l *redislocker) Unlock(ctx context.Context, queueIDs ...string) error {
	members := make([]interface{}, len(queueIDs))
	for _, queueID := range queueIDs {
		members = append(members, queueID)
	}
	return l.cli.ZRem(ctx, l.keyName, members...).Err()
}
