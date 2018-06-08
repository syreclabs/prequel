package prequel

import "syreclabs.com/go/loggie"

// Logger is a logging interface user by prequel.
type Logger interface {
	Printf(format string, v ...interface{})
}

// defaultLogger is a logging adapter which uses loggie for logging.
type defaultLogger struct {
	logger loggie.Logger
}

const defaultLoggerName = "sql"

func newDefaultLogger() Logger {
	return &defaultLogger{loggie.New(defaultLoggerName)}
}

func (l *defaultLogger) Printf(format string, v ...interface{}) {
	l.logger.Infof(format, v...)
}
