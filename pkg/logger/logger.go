package logger

import (
	"fmt"
	"log"
	"os"
)

const (
	debug string = "[DEBUG]"
	info  string = "[INFO]"
	warn  string = "[WARN]"
	err   string = "[ERROR]"
	fatal string = "[FATAL]"
)

func Debug(format string, args ...any) {
	logInternal(debug, format, args...)
}

func Info(format string, args ...any) {
	logInternal(info, format, args...)
}

func Warn(format string, args ...any) {
	logInternal(warn, format, args...)
}

func Error(format string, args ...any) {
	logInternal(err, format, args...)
}

func Fatal(exitStatus int, format string, args ...any) {
	logInternal(fatal, format, args...)
	os.Exit(exitStatus)
}

func logInternal(level, format string, args ...any) {
	msg := level + fmt.Sprintf(format, args...)
	log.Print(msg)
}
