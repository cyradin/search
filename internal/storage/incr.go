package storage

import "context"

func Increment(ctx context.Context, key string) (int64, error) {
	resp := Redis(ctx).Incr(ctx, key)

	return resp.Result()
}

func IncrementBy(ctx context.Context, key string, val int64) (int64, error) {
	resp := Redis(ctx).IncrBy(ctx, key, val)

	return resp.Result()
}
