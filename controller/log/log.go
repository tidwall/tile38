package log

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh/terminal"
)

var mu sync.Mutex
var wr io.Writer
var tty bool

// Level is the log level
// 0: silent  - do not log
// 1: normal  - show everything except debug and warn
// 2: verbose - show everything except debug
// 3: very verbose - show everything
var Level = 1

// SetOutput sets the output of the logger
func SetOutput(w io.Writer) {
	f, ok := w.(*os.File)
	tty = ok && terminal.IsTerminal(int(f.Fd()))
	wr = w
}

func log(level int, tag, color string, formatted bool, format string, args ...interface{}) {
	if Level < level {
		return
	}
	s := []byte(time.Now().Format("2006/01/02 15:04:05"))
	s = append(s, ' ')
	if tty {
		s = append(s, color...)
	}
	s = append(s, '[')
	s = append(s, tag...)
	s = append(s, ']')
	if tty {
		s = append(s, "\x1b[0m"...)
	}
	s = append(s, ' ')
	if formatted {
		s = append(s, fmt.Sprintf(format, args...)...)
	} else {
		s = append(s, fmt.Sprint(args...)...)
	}
	if s[len(s)-1] != '\n' {
		s = append(s, '\n')
	}
	mu.Lock()
	wr.Write(s)
	mu.Unlock()
}

func Infof(format string, args ...interface{}) {
	log(1, "INFO", "\x1b[36m", true, format, args...)
}
func Info(args ...interface{}) {
	log(1, "INFO", "\x1b[36m", false, "", args...)
}
func HTTPf(format string, args ...interface{}) {
	log(1, "HTTP", "\x1b[1m\x1b[30m", true, format, args...)
}
func HTTP(args ...interface{}) {
	log(1, "HTTP", "\x1b[1m\x1b[30m", false, "", args...)
}
func Errorf(format string, args ...interface{}) {
	log(1, "ERRO", "\x1b[1m\x1b[31m", true, format, args...)
}
func Error(args ...interface{}) {
	log(1, "ERRO", "\x1b[1m\x1b[31m", false, "", args...)
}
func Warnf(format string, args ...interface{}) {
	log(2, "WARN", "\x1b[33m", true, format, args...)
}
func Warn(args ...interface{}) {
	log(2, "WARN", "\x1b[33m", false, "", args...)
}
func Debugf(format string, args ...interface{}) {
	log(3, "DEBU", "\x1b[35m", true, format, args...)
}
func Debug(args ...interface{}) {
	log(3, "DEBU", "\x1b[35m", false, "", args...)
}
func Printf(format string, args ...interface{}) {
	Infof(format, args...)
}
func Print(format string, args ...interface{}) {
	Info(args...)
}
func Fatalf(format string, args ...interface{}) {
	log(1, "FATA", "x1b[31m", true, format, args...)
	os.Exit(1)
}
func Fatal(args ...interface{}) {
	log(1, "FATA", "\x1b[31m", false, "", args...)
	os.Exit(1)
}
