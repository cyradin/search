package storage

import (
	"context"
	"errors"

	"github.com/cyradin/search/internal/errs"
	"github.com/go-redis/redis/v9"
	jsoniter "github.com/json-iterator/go"
)

func DictGet[T any](ctx context.Context, key string, id string) (T, error) {
	resp := Redis(ctx).HGet(ctx, makeKey(ctx, key), id)

	var result T

	if err := resp.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrNotFound
		}
		return result, errs.Errorf("storage get err: %w", err)
	}

	if err := jsoniter.Unmarshal([]byte(resp.Val()), &result); err != nil {
		return result, errs.Errorf("storage unmarshal err: %w", err)
	}

	return result, nil
}

func DictSet[T any](ctx context.Context, key string, id string, value interface{}) error {
	vv, err := jsoniter.Marshal(value)
	if err != nil {
		return errs.Errorf("storage marshal err: %w", err)
	}

	resp := Redis(ctx).HSetNX(ctx, makeKey(ctx, key), id, string(vv))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func DictKeys[T any](ctx context.Context, key string) ([]string, error) {
	resp := Redis(ctx).HKeys(ctx, makeKey(ctx, key))
	if err := resp.Err(); err != nil {
		return nil, errs.Errorf("storage get keys err: %w", err)
	}

	return resp.Val(), nil
}

func DictValues[T any](ctx context.Context, key string) ([]T, error) {
	resp := Redis(ctx).HGetAll(ctx, makeKey(ctx, key))
	if err := resp.Err(); err != nil {
		return nil, errs.Errorf("storage get all err: %w", err)
	}

	dst := make([]T, 0, 64)
	for _, v := range resp.Val() {
		var vv T
		if err := jsoniter.Unmarshal([]byte(v), &vv); err != nil {
			return nil, errs.Errorf("storage unmarshal err: %w", err)
		}
		dst = append(dst, vv)
	}

	return dst, nil
}

func DictAll[T any](ctx context.Context, key string, dst map[string]T) error {
	resp := Redis(ctx).HGetAll(ctx, makeKey(ctx, key))
	if err := resp.Err(); err != nil {
		return errs.Errorf("storage get all err: %w", err)
	}

	for k, v := range resp.Val() {
		var vv T
		if err := jsoniter.Unmarshal([]byte(v), &vv); err != nil {
			return errs.Errorf("storage unmarshal err: %w", err)
		}
		dst[k] = vv
	}

	return nil
}

func DictDel[T any](ctx context.Context, key string, id string) error {
	resp := Redis(ctx).HDel(ctx, makeKey(ctx, key), id)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage del err: %w", err)
	}

	if resp.Val() == 0 {
		return ErrNotFound
	}

	return nil
}
