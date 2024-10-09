package log

import (
	"fmt"
	"log"
	"os"
)

const (
	stopColor = "\033[0m"
	gray      = "\033[90m"
	red       = "\033[91m"
	green     = "\033[92m"
	yellow    = "\033[93m"
	cyan      = "\033[36m"
	darkRed   = "\033[31m"
)

func Debug(msg string, args ...any) {
	logWrapper("Debug", cyan, msg, args...)
}

func Info(msg string, args ...any) {
	logWrapper("Info", green, msg, args...)
}

func Warn(msg string, args ...any) {
	logWrapper("Warn", yellow, msg, args...)
}

func Error(msg string, args ...any) {
	logWrapper("Error", red, msg, args...)
}

// Logs the message and terminates the program with status code 1.
func Fatal(msg string, args ...any) {
	logWrapper("FATAL", darkRed, msg, args...)
	os.Exit(1)
}

func logWrapper(severity, color, msg string, args ...any) {
	log.Printf("%s[%s]%s %s",
		color, severity, stopColor,
		fmt.Sprintf(msg, args...),
	)
}
