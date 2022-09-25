package sqsd

import (
	"encoding"
	"fmt"
	"log"
	"strings"
)

// LogLevel sets log level for sqsd.
type LogLevel uint32

const (
	// DebugLevel sets debug log level.
	DebugLevel LogLevel = iota + 1
	// InfoLevel sets info log level.
	InfoLevel
	// WarnLevel sets warn log level.
	WarnLevel
	// ErrorLevel sets error log level.
	ErrorLevel
)

var _ encoding.TextUnmarshaler = (*LogLevel)(nil)

var levelMap = map[string]LogLevel{
	"debug": DebugLevel,
	"info":  InfoLevel,
	"warn":  WarnLevel,
	"error": ErrorLevel,
}

// UnmarshalText scans levelMap from supplied text and set scanned level to LogLevel.
func (l *LogLevel) UnmarshalText(text []byte) error {
	lv, ok := levelMap[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("invalid log_level: %s", text)
	}
	*l = lv
	return nil
}

// Logging hides logger implementation.
type Logging interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

var logger = NewLogger(InfoLevel, "[sqsd]")

// SetLogLevel set supplied log.Level in actor, mailbox and our logger.
func SetLogLevel(ll LogLevel) {
	if l, ok := logger.(*defaultLogger); ok {
		l.logLevel = ll
	}
}

// SetLogger sets another logger to change behavior from default.
func SetLogger(l Logging) {
	logger = l
}

// NewField returns new Field.
func NewField(key string, val interface{}) Field {
	return Field{key, val}
}

// Field has key and value for logging.
type Field struct {
	key string
	val interface{}
}

type defaultLogger struct {
	logLevel LogLevel
	prefix   string
	logger   *log.Logger
}

// NewLogger returns new logger for default behavior.
func NewLogger(lv LogLevel, prefix string, loggers ...*log.Logger) Logging {
	l := log.Default()
	if len(loggers) > 0 {
		l = loggers[0]
	}
	return &defaultLogger{lv, prefix, l}
}

func (l *defaultLogger) print(lv string, msg string, fields []Field) {
	fv := ""
	for _, kv := range fields {
		fv += fmt.Sprintf("\t%s=%v", kv.key, kv.val)
	}
	prefix := l.prefix
	if prefix != "" {
		prefix += " "
	}
	l.logger.Printf("[%s] %s%s %s", lv, prefix, msg, fv)
}

func (l *defaultLogger) Debug(msg string, fields ...Field) {
	if l.logLevel >= DebugLevel {
		l.print("DEBUG", msg, fields)
	}
}

func (l *defaultLogger) Info(msg string, fields ...Field) {
	if l.logLevel >= InfoLevel {
		l.print("INFO", msg, fields)
	}
}

func (l *defaultLogger) Warn(msg string, fields ...Field) {
	if l.logLevel >= WarnLevel {
		l.print("WARN", msg, fields)
	}
}

func (l *defaultLogger) Error(msg string, fields ...Field) {
	if l.logLevel >= ErrorLevel {
		l.print("ERROR", msg, fields)
	}
}
