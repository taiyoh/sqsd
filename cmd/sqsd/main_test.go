package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigWithoutRedisLocker(t *testing.T) {
	var conf config
	os.Setenv("INVOKER_URL", "http://localhost:8080")
	os.Setenv("QUEUE_URL", "http://localhost:8080")
	t.Cleanup(func() {
		os.Clearenv()
	})

	assert.NoError(t, conf.Load())
	assert.Nil(t, conf.RedisLocker)
}

func TestConfigWithRedisLocker(t *testing.T) {
	os.Setenv("INVOKER_URL", "http://localhost:8080")
	os.Setenv("QUEUE_URL", "http://localhost:8080")
	os.Setenv("REDIS_LOCKER_HOST", "localhost:6739")

	t.Cleanup(func() {
		os.Clearenv()
	})

	t.Run("redis locker variables are not enough", func(t *testing.T) {
		var conf config
		assert.NoError(t, conf.Load())
		assert.Nil(t, conf.RedisLocker)
	})

	t.Run("redis locker variables are sets", func(t *testing.T) {
		var conf config
		os.Setenv("REDIS_LOCKER_DBNAME", "3")
		os.Setenv("REDIS_LOCKER_KEYNAME", "hogefuga")
		assert.NoError(t, conf.Load())
		assert.Equal(t, redisLocker{
			Host:    "localhost:6739",
			DBName:  3,
			KeyName: "hogefuga",
		}, *conf.RedisLocker)
	})
}
