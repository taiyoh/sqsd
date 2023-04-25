package sqsd

import (
	"io"
	"os"
	"sync/atomic"

	"golang.org/x/exp/slog"
)

var logger atomic.Value

// NewLogger constructs slog.Logger object with default 'system' attribute.
func NewLogger(lv slog.Level, w io.Writer, system string) *slog.Logger {
	opt := slog.HandlerOptions{
		Level: lv,
	}

	return slog.New(opt.NewJSONHandler(w).WithAttrs([]slog.Attr{
		{
			Key:   "system",
			Value: slog.StringValue(system),
		},
	}))
}

// SetWithGlobalLevel sets default logger with supplied log level.
// if io.Writer is not supplied, use os.Stderr.
func SetWithGlobalLevel(lv slog.Level, writers ...io.Writer) {
	var w io.Writer = os.Stderr
	if len(writers) > 0 {
		w = writers[0]
	}
	logger.Store(NewLogger(lv, w, "sqsd"))
}

func getLogger() *slog.Logger {
	return logger.Load().(*slog.Logger)
}
