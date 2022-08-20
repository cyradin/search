package storage

import (
	"context"
	"errors"

	"github.com/cyradin/search/internal/errs"
	"github.com/go-redis/redis/v9"
	jsoniter "github.com/json-iterator/go"
)

type DictStorage[T any] struct {
	client *redis.Client
	prefix string
}

func NewDictStorage[T any](redisClient *redis.Client) *DictStorage[T] {
	return &DictStorage[T]{
		client: redisClient,
	}
}

func (s *DictStorage[T]) WithPrefix(prefix string) *DictStorage[T] {
	return &DictStorage[T]{
		client: s.client,
		prefix: makeKey(s.prefix, prefix),
	}
}

func (s *DictStorage[T]) WithKey(key string) *KeyedDictStorage[T] {
	return &KeyedDictStorage[T]{
		storage: s,
		key:     key,
	}
}

func (s *DictStorage[T]) Get(ctx context.Context, key string, id string) (T, error) {
	resp := s.client.HGet(ctx, makeKey(s.prefix, key), id)

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

func (s *DictStorage[T]) Set(ctx context.Context, key string, id string, value interface{}) error {
	vv, err := jsoniter.Marshal(value)
	if err != nil {
		return errs.Errorf("storage marshal err: %w", err)
	}

	resp := s.client.HSetNX(ctx, makeKey(s.prefix, key), id, string(vv))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func (s *DictStorage[T]) Keys(ctx context.Context, key string) ([]string, error) {
	resp := s.client.HKeys(ctx, makeKey(s.prefix, key))
	if err := resp.Err(); err != nil {
		return nil, errs.Errorf("storage get keys err: %w", err)
	}

	return resp.Val(), nil
}

func (s *DictStorage[T]) AllValues(ctx context.Context, key string) ([]T, error) {
	resp := s.client.HGetAll(ctx, makeKey(s.prefix, key))
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

func (s *DictStorage[T]) All(ctx context.Context, key string, dst map[string]T) error {
	resp := s.client.HGetAll(ctx, makeKey(s.prefix, key))
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

func (s *DictStorage[T]) Del(ctx context.Context, key string, id string) error {
	resp := s.client.HDel(ctx, makeKey(s.prefix, key), id)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage del err: %w", err)
	}

	if resp.Val() == 0 {
		return ErrNotFound
	}

	return nil
}

type KeyedDictStorage[T any] struct {
	storage *DictStorage[T]
	key     string
}

func (s *KeyedDictStorage[T]) Get(ctx context.Context, id string) (T, error) {
	return s.storage.Get(ctx, s.key, id)
}

func (s *KeyedDictStorage[T]) Set(ctx context.Context, id string, value interface{}) error {
	return s.storage.Set(ctx, s.key, id, value)
}

func (s *KeyedDictStorage[T]) Keys(ctx context.Context) ([]string, error) {
	return s.storage.Keys(ctx, s.key)
}

func (s *KeyedDictStorage[T]) AllValues(ctx context.Context) ([]T, error) {
	return s.storage.AllValues(ctx, s.key)
}

func (s *KeyedDictStorage[T]) All(ctx context.Context, dst map[string]T) error {
	return s.storage.All(ctx, s.key, dst)
}

func (s *KeyedDictStorage[T]) Del(ctx context.Context, id string) error {
	return s.storage.Del(ctx, s.key, id)
}
