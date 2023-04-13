package util

import (
	"context"

	"golang.org/x/exp/slog"
)

type loggerKey struct{}

func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func LoggerFromContext(ctx context.Context) *slog.Logger {
	logger := ctx.Value(loggerKey{})
	if logger == nil {
		return slog.Default()
	} else {
		return logger.(*slog.Logger)
	}
}
