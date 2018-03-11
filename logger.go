package sqsd

import (
	"log"
	"os"

	"github.com/hashicorp/logutils"
)

type Logger interface {
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type logger struct {
	Logger
	logger *log.Logger
}

func NewLogger(logLevel string) *logger {
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
func (l *logger) Info(msg string) {
	l.logger.Println("[INFO] " + msg)
}
func (l *logger) Warn(msg string) {
	l.logger.Println("[WARN] " + msg)
}
func (l *logger) Error(msg string) {
	l.logger.Println("[ERROR] " + msg)
}
