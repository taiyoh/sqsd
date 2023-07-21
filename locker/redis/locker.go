package redislocker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
	"github.com/taiyoh/sqsd/locker"
)

type redislocker struct {
	keyName string
	cli     rueidis.Client
}

var _ locker.QueueLocker = (*redislocker)(nil)

// New creates QueueLocker by Redis.
func New(cli rueidis.Client, keyName string) locker.QueueLocker {
	return &redislocker{
		keyName: keyName,
		cli:     cli,
	}
}

func (l *redislocker) Lock(ctx context.Context, queueID string) error {
	now := time.Now().UTC()
	cmd := l.cli.B().Zadd().Key(l.keyName).Nx().ScoreMember().ScoreMember(
		float64(now.UnixNano()),
		queueID,
	)
	result := l.cli.Do(ctx, cmd.Build())
	if err := result.Error(); err != nil {
		return err
	}
	affected, err := result.AsInt64()
	if err != nil {
		return err
	}
	if affected == 0 {
		return locker.ErrQueueExists
	}
	return nil
}

func (l *redislocker) Unlock(ctx context.Context, ts time.Time) error {
	cmd := l.cli.B().Zremrangebyscore().Key(l.keyName).Min("-inf").Max(fmt.Sprintf("%d", ts.UnixNano()))
	return l.cli.Do(ctx, cmd.Build()).Error()
}
