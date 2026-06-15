package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

var Log zerolog.Logger
var Audit zerolog.Logger

func Init(logLevel string) {
	level := zerolog.InfoLevel
	switch logLevel {
	case "debug":
		level = zerolog.DebugLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	}
	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339Nano

	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05.000",
	}
	Log = zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()

	auditFile, err := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		Log.Warn().Err(err).Msg("failed to open audit.log, using stdout")
		Audit = zerolog.New(os.Stdout).With().Timestamp().Str("component", "audit").Logger()
	} else {
		Audit = zerolog.New(auditFile).With().Timestamp().Str("component", "audit").Logger()
	}
}

func RecordAudit(user, action, detail string) {
	Audit.Info().
		Str("user", user).
		Str("action", action).
		Str("detail", detail).
		Msg("AUDIT")
}
