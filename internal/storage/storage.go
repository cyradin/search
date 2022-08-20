package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyradin/search/internal/errs"
	"github.com/go-redis/redis/v9"
)

var ErrNotFound = fmt.Errorf("not found")

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

func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	resp := s.client.Get(ctx, makeKey(s.prefix, key))

	if err := resp.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrNotFound
		}
		return "", errs.Errorf("storage get err: %w", err)
	}

	return resp.Val(), nil
}

func (s *Storage) Set(ctx context.Context, key string, value string) error {
	resp := s.client.Set(ctx, makeKey(s.prefix, key), value, 0)

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

type KeyedStorage struct {
	storage *Storage
	key     string
}

func (s *KeyedStorage) Get(ctx context.Context) (string, error) {
	return s.storage.Get(ctx, s.key)
}

func (s *KeyedStorage) Set(ctx context.Context, value string) error {
	return s.storage.Set(ctx, s.key, value)
}

func (s *KeyedStorage) Del(ctx context.Context) error {
	return s.storage.Del(ctx, s.key)
}

func makeKey(prefix string, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "|" + key
}
