package storage

import "context"

type KeyedStorage struct {
	storage *Storage
	key     string
}

func (s *KeyedStorage) GetString(ctx context.Context) (string, error) {
	return s.storage.GetString(ctx, s.key)
}
func (s *KeyedStorage) SetString(ctx context.Context, value string) error {
	return s.storage.SetString(ctx, s.key, value)
}
func (s *KeyedStorage) GetJSON(ctx context.Context, dst interface{}) error {
	return s.storage.GetJSON(ctx, s.key, dst)
}
func (s *KeyedStorage) SetJSON(ctx context.Context, value interface{}) error {
	return s.storage.SetJSON(ctx, s.key, value)
}
func (s *KeyedStorage) GetDictString(ctx context.Context, id string) (string, error) {
	return s.storage.GetDictString(ctx, s.key, id)
}
func (s *KeyedStorage) SetDictString(ctx context.Context, id string, value string) error {
	return s.storage.SetDictString(ctx, s.key, id, value)
}
func (s *KeyedStorage) GetDictJSON(ctx context.Context, id string, dst interface{}) error {
	return s.storage.GetDictJSON(ctx, s.key, id, dst)
}
func (s *KeyedStorage) SetDictJSON(ctx context.Context, id string, value interface{}) error {
	return s.storage.SetDictJSON(ctx, s.key, id, value)
}

func (s *KeyedStorage) Del(ctx context.Context) error {
	return s.storage.Del(ctx, s.key)
}

func (s *KeyedStorage) DelDict(ctx context.Context, id string) error {
	return s.storage.DelDict(ctx, s.key, id)
}
