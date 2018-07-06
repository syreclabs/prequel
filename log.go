package prequel

import "syreclabs.com/go/loggie"

// Logger is a logging interface user by prequel.
type Logger interface {
	Printf(format string, v ...interface{})
	SetLevel(lvl int)
}

// defaultLogger is a logging adapter which uses syreclabs.com/go/loggie
// for logging. Use SetLogger() to change.
type defaultLogger struct {
	logger loggie.Logger
}

const defaultLoggerName = "sql"

func newDefaultLogger() Logger {
	return &defaultLogger{loggie.New(defaultLoggerName)}
}

func (l *defaultLogger) Printf(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}

func (l *defaultLogger) SetLevel(lvl int) {
	l.logger.SetLevel(lvl)
}
