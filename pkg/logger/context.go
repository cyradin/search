package logger

import (
	"context"

	"go.uber.org/zap"
)

type ctxKey string

const (
	ctxKeyLogger ctxKey = "logger"

	ctxKeyRequestRoute  ctxKey = "req_route"
	ctxKeyRequestMethod ctxKey = "req_method"
	ctxKeyRequestID     ctxKey = "req_id"
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

func WithRequestMethod(ctx context.Context, v string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestMethod, v)
}

func RequestMethod(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestMethod).(string); ok {
		return v
	}
	return ""
}

func WithRequestID(ctx context.Context, v string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, v)
}

func RequestID(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

func WithRequestRoute(ctx context.Context, v string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestRoute, v)
}

func RequestRoute(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestRoute).(string); ok {
		return v
	}
	return ""
}
