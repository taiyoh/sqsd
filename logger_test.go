package sqsd

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v6"
	"github.com/stretchr/testify/assert"
)

func TestLogLevel(t *testing.T) {
	t.Cleanup(func() {
		os.Unsetenv("LOG_LEVEL")
	})

	type testcfg struct {
		LogLevel LogLevel `env:"LOG_LEVEL" envDefault:"info"`
	}

	t.Run("default assigned", func(t *testing.T) {
		c := testcfg{}
		assert.NoError(t, env.Parse(&c))
		assert.Equal(t, InfoLevel, c.LogLevel)
	})
	t.Run("`DEBUG` is assigned", func(t *testing.T) {
		os.Setenv("LOG_LEVEL", "DEBUG")
		c := testcfg{}
		assert.NoError(t, env.Parse(&c))
		assert.Equal(t, DebugLevel, c.LogLevel)
	})
	t.Run("`SQSDTEST` is assigned", func(t *testing.T) {
		os.Setenv("LOG_LEVEL", "SQSDTEST")
		c := testcfg{}
		assert.Error(t, env.Parse(&c))
	})
}
