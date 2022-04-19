package ctxt

import (
	"context"
	"sync"
)

type ctxKey string

const (
	ctxKeyLogger    ctxKey = "logger"
	ctxKeyWaitGroup ctxKey = "wg"

	ctxKeyRequestRoute  ctxKey = "req_route"
	ctxKeyRequestMethod ctxKey = "req_method"
	ctxKeyRequestID     ctxKey = "req_id"
)

func WithWg(ctx context.Context, v *sync.WaitGroup) context.Context {
	return context.WithValue(ctx, ctxKeyWaitGroup, v)
}

func Wg(ctx context.Context) *sync.WaitGroup {
	if v, ok := ctx.Value(ctxKeyWaitGroup).(*sync.WaitGroup); ok {
		return v
	}
	return &sync.WaitGroup{}
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
