package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cyradin/search/internal/errs"
	"github.com/go-redis/redis/v9"
)

var ErrNotFound = fmt.Errorf("not found")

func Get(ctx context.Context, key string) (string, error) {
	resp := Redis(ctx).Get(ctx, makeKey(ctx, key))

	if err := resp.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrNotFound
		}
		return "", errs.Errorf("storage get err: %w", err)
	}

	return resp.Val(), nil
}

func Set(ctx context.Context, key string, value string) error {
	resp := Redis(ctx).Set(ctx, makeKey(ctx, key), value, 0)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func Del(ctx context.Context, key string) error {
	resp := Redis(ctx).Del(ctx, makeKey(ctx, key))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage del err: %w", err)
	}

	return nil
}

func makeKey(ctx context.Context, parts ...string) string {
	result := strings.Join(parts, "|")
	if prefix := GlobalPrefix(ctx); prefix != "" {
		if result == "" {
			return prefix
		}
		result = prefix + "|" + result
	}
	return result
}
