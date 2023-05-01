package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigWithoutRedisLocker(t *testing.T) {
	var conf sqsdConfig
	t.Setenv("INVOKER_URL", "http://localhost:8080")
	t.Setenv("QUEUE_URL", "http://localhost:8080")

	assert.NoError(t, conf.Load())
	assert.Nil(t, conf.RedisLocker)
}

func TestConfigWithRedisLocker(t *testing.T) {
	t.Setenv("INVOKER_URL", "http://localhost:8080")
	t.Setenv("QUEUE_URL", "http://localhost:8080")
	t.Setenv("REDIS_LOCKER_HOST", "localhost:6739")

	t.Run("redis locker variables are not enough", func(t *testing.T) {
		var conf sqsdConfig
		assert.NoError(t, conf.Load())
		assert.Nil(t, conf.RedisLocker)
	})

	t.Run("redis locker variables are sets", func(t *testing.T) {
		var conf sqsdConfig
		t.Setenv("REDIS_LOCKER_DBNAME", "3")
		t.Setenv("REDIS_LOCKER_KEYNAME", "hogefuga")
		assert.NoError(t, conf.Load())
		assert.Equal(t, redisLocker{
			Host:    "localhost:6739",
			DBName:  3,
			KeyName: "hogefuga",
		}, *conf.RedisLocker)
	})
}
