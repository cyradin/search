package storage

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v9"
)

type ctxKey string

const (
	ctxRedis  ctxKey = "storage_redis_client"
	ctxPrefix ctxKey = "storage_global_prefix"
)

func WithRedis(ctx context.Context, c *redis.Client) context.Context {
	return context.WithValue(ctx, ctxRedis, c)
}

func Redis(ctx context.Context) *redis.Client {
	return ctx.Value(ctxRedis).(*redis.Client)
}

func WithGlobalPrefix(ctx context.Context, prefix string) context.Context {
	return context.WithValue(ctx, ctxPrefix, prefix)
}

func GlobalPrefix(ctx context.Context) string {
	if s, ok := ctx.Value(ctxPrefix).(string); ok {
		return s
	}

	return ""
}

func PrefixIndexes() string {
	return "indexes"
}

func PrefixIndexIDs(name string) string {
	return fmt.Sprintf("index|%s|ids", name)
}
