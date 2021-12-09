// in order to debug easier, inspired by etcd and extended based on it
package raft

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type Logger interface {
	//personal test mode ---> for single test instead of fmt.Println
	Test(...interface{})
	Testf(string, ...interface{})

	Debug(...interface{})
	Debugf(string, ...interface{})

	Info(...interface{})
	Infof(string, ...interface{})

	Error(...interface{})
	Errorf(string, ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(...interface{})
	Fatalf(string, ...interface{})

	Panic(...interface{})
	Panicf(string, ...interface{})
}

var (
	defaultLogger = &DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	// build a logger to write log in a file
	f, err      = os.OpenFile("raftLog.txt", os.O_CREATE|os.O_APPEND, 0666)
	writeLogger = &DefaultLogger{Logger: log.New(f, "rafttest", log.LstdFlags)}

	testLogger  = &DefaultLogger{}
	loggerMutex sync.Mutex
	raftLogger  = Logger(defaultLogger)
)

const (
	calldepth = 2
)

func SetLogger(l Logger) {
	loggerMutex.Lock()
	raftLogger = l
	loggerMutex.Unlock()
}

func ResetDefaultLogger() {
	SetLogger(defaultLogger)
}

func getLogger() Logger {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	return raftLogger
}

// ----> learn from etcd
type DefaultLogger struct {
	*log.Logger
	debug bool
	test  bool
}

func (l *DefaultLogger) EnableTimestamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}

func (l *DefaultLogger) EnableDebug() {
	l.debug = true
}

// ----> enable single test
func (l *DefaultLogger) EnableTest() {
	l.test = true
}

func (l *DefaultLogger) Test(v ...interface{}) {
	if l.test {
		l.Output(calldepth, header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Testf(format string, v ...interface{}) {
	if l.test {
		l.Output(calldepth, header("TEST", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Info(v ...interface{}) {
	l.Output(calldepth, header("INFO", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	l.Output(calldepth, header("INFO", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Error(v ...interface{}) {
	l.Output(calldepth, header("ERROR", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	l.Output(calldepth, header("ERROR", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Warning(v ...interface{}) {
	l.Output(calldepth, header("WARN", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Warningf(format string, v ...interface{}) {
	l.Output(calldepth, header("WARN", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Fatal(v ...interface{}) {
	l.Output(calldepth, header("FATAL", fmt.Sprint(v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	l.Output(calldepth, header("FATAL", fmt.Sprintf(format, v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Panic(v ...interface{}) {
	l.Logger.Panic(v...)
}

func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}
