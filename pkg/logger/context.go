package logger

import (
	"context"

	"go.uber.org/zap"
)

type ctxKey string

const (
	ctxKeyLogger ctxKey = "logger"
)

var noOpLogger = zap.NewNop()

func WithContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, logger)
}

func FromContext(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(ctxKeyLogger).(*zap.Logger); ok {
		return l
	}
	return noOpLogger
}

func ExtractFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	// @todo get fields from context
	return fields
}
