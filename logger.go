package sqsd

import (
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/logutils"
)

// Logger is interface for sqsd logging object
type Logger interface {
	Debug(msg string)
	Debugf(tmpl string, msgs ...interface{})
	Info(msg string)
	Infof(tmpl string, msgs ...interface{})
	Warn(msg string)
	Warnf(tmpl string, msgs ...interface{})
	Error(msg string)
	Errorf(tmpl string, msgs ...interface{})
}

type logger struct {
	Logger
	logger *log.Logger
}

// NewLogger returns Logger implementation
func NewLogger(logLevel string) Logger {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel(logLevel),
		Writer:   os.Stderr,
	}
	return &logger{
		logger: log.New(filter, "", log.LstdFlags),
	}
}

func (l *logger) Debug(msg string) {
	l.logger.Println("[DEBUG] " + msg)
}

func (l *logger) Debugf(format string, a ...interface{}) {
	l.Debug(fmt.Sprintf(format, a...))
}

func (l *logger) Info(msg string) {
	l.logger.Println("[INFO] " + msg)
}

func (l *logger) Infof(format string, a ...interface{}) {
	l.Info(fmt.Sprintf(format, a...))
}

func (l *logger) Warn(msg string) {
	l.logger.Println("[WARN] " + msg)
}

func (l *logger) Warnf(format string, a ...interface{}) {
	l.Warn(fmt.Sprintf(format, a...))
}

func (l *logger) Error(msg string) {
	l.logger.Println("[ERROR] " + msg)
}

func (l *logger) Errorf(format string, a ...interface{}) {
	l.Error(fmt.Sprintf(format, a...))
}
