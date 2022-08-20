package storage

import (
	"context"
	"fmt"

	"github.com/cyradin/search/internal/errs"
	"github.com/go-redis/redis/v9"
	jsoniter "github.com/json-iterator/go"
)

var ErrNotFound = fmt.Errorf("todo")

type Storage struct {
	client *redis.Client
	prefix string
}

func New(client *redis.Client, prefix string) *Storage {
	return &Storage{
		client: client,
	}
}

func (s *Storage) WithPrefix(prefix string) *Storage {
	return &Storage{
		client: s.client,
		prefix: makeKey(s.prefix, prefix),
	}
}

func (s *Storage) WithKey(key string) *KeyedStorage {
	return &KeyedStorage{
		storage: s,
		key:     key,
	}
}

func (s *Storage) GetString(ctx context.Context, key string) (string, error) {
	resp := s.client.Get(ctx, makeKey(s.prefix, key))

	if err := resp.Err(); err != nil {
		return "", errs.Errorf("storage get err: %w", err)
	}

	return resp.Val(), nil
}

func (s *Storage) SetString(ctx context.Context, key string, value string) error {
	resp := s.client.Set(ctx, makeKey(s.prefix, key), value, 0)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func (s *Storage) GetJSON(ctx context.Context, key string, dst interface{}) error {
	resp := s.client.Get(ctx, makeKey(s.prefix, key))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage get err: %w", err)
	}

	if err := jsoniter.Unmarshal([]byte(resp.Val()), dst); err != nil {
		return errs.Errorf("storage get err: %w", err)
	}

	return nil
}

func (s *Storage) SetJSON(ctx context.Context, key string, value interface{}) error {
	vv, err := jsoniter.Marshal(value)

	if err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	resp := s.client.Set(ctx, makeKey(s.prefix, key), string(vv), 0)
	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func (s *Storage) GetDictString(ctx context.Context, key string, id string) (string, error) {
	resp := s.client.HGet(ctx, makeKey(s.prefix, key), id)

	if err := resp.Err(); err != nil {
		return "", errs.Errorf("storage get err: %w", err)
	}

	return resp.Val(), nil
}

func (s *Storage) SetDictString(ctx context.Context, key string, id string, value string) error {
	resp := s.client.HSetNX(ctx, makeKey(s.prefix, key), id, value)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func (s *Storage) GetDictJSON(ctx context.Context, key string, id string, dst interface{}) error {
	resp := s.client.HGet(ctx, makeKey(s.prefix, key), id)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage get err: %w", err)
	}

	if err := jsoniter.Unmarshal([]byte(resp.Val()), dst); err != nil {
		return errs.Errorf("storage get err: %w", err)
	}

	return nil
}

func (s *Storage) SetDictJSON(ctx context.Context, key string, id string, value interface{}) error {
	vv, err := jsoniter.Marshal(value)

	if err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	resp := s.client.HSetNX(ctx, makeKey(s.prefix, key), id, string(vv))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage set err: %w", err)
	}

	return nil
}

func (s *Storage) Del(ctx context.Context, key string) error {
	resp := s.client.Del(ctx, makeKey(s.prefix, key))

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage del err: %w", err)
	}

	return nil
}

func (s *Storage) DelDict(ctx context.Context, key string, id string) error {
	resp := s.client.HDel(ctx, makeKey(s.prefix, key), id)

	if err := resp.Err(); err != nil {
		return errs.Errorf("storage del err: %w", err)
	}

	return nil
}

func makeKey(prefix string, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "|" + key
}
