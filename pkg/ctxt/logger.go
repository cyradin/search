package ctxt

import (
	"context"

	"go.uber.org/zap"
)

var noOpLogger = zap.NewNop()

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, logger)
}

func Logger(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(ctxKeyLogger).(*zap.Logger); ok {
		return l
	}
	return noOpLogger
}

func ExtractFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	if v := RequestMethod(ctx); v != "" {
		fields = append(fields, zap.String(string(ctxKeyRequestMethod), v))
	}
	if v := RequestRoute(ctx); v != "" {
		fields = append(fields, zap.String(string(ctxKeyRequestRoute), v))
	}
	if v := RequestID(ctx); v != "" {
		fields = append(fields, zap.String(string(ctxKeyRequestID), v))
	}

	return fields
}
