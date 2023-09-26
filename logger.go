package sqsd

import (
	"io"
	"log/slog"
	"os"
	"sync/atomic"
)

var logger atomic.Value

// NewLogger constructs slog.Logger object with default 'system' attribute.
func NewLogger(handlerOpts slog.HandlerOptions, w io.Writer, system string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &handlerOpts).WithAttrs([]slog.Attr{
		{
			Key:   "system",
			Value: slog.StringValue(system),
		},
	}))
}

// SetWithHandlerOptions sets default logger with slog handler options.
// if io.Writer is not supplied, use os.Stderr.
func SetWithHandlerOptions(handlerOpts slog.HandlerOptions, writers ...io.Writer) {
	var w io.Writer = os.Stderr
	if len(writers) > 0 {
		w = writers[0]
	}
	logger.Store(NewLogger(handlerOpts, w, "sqsd"))
}

func getLogger() *slog.Logger {
	return logger.Load().(*slog.Logger)
}
