package ctxt

import (
	"context"
)

type ctxKey string

const (
	ctxKeyLogger ctxKey = "logger"

	ctxKeyRequestRoute  ctxKey = "req_route"
	ctxKeyRequestMethod ctxKey = "req_method"
	ctxKeyRequestID     ctxKey = "req_id"
)

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
