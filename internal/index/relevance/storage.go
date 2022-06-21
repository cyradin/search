package relevance

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/logger"
	"go.uber.org/zap"
)

const relevanceFile = "relevance.json"
const dirPermissions = 0755
const filePermissions = 0644

type Storage struct {
	src     string
	mtx     sync.RWMutex
	indexes map[string]*Index
}

func NewStorage(src string) *Storage {
	result := &Storage{
		src:     src,
		indexes: make(map[string]*Index),
	}

	events.Subscribe(events.NewAppStop(), func(ctx context.Context, e events.Event) {
		result.mtx.Lock()
		defer result.mtx.Unlock()
		for _, i := range result.indexes {
			if err := i.dump(); err != nil {
				logger.FromCtx(ctx).Error("relevance.index.dump.error", logger.ExtractFields(ctx, zap.Error(err))...)
			}
		}
	})

	return result
}

func (s *Storage) AddIndex(name string) (*Index, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, ok := s.indexes[name]; ok {
		return nil, fmt.Errorf("index %q aready initialized", name)
	}

	dir := path.Join(s.src, name)
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return nil, fmt.Errorf("index dir %q create err: %w", dir, err)
	}
	src := path.Join(dir, relevanceFile)

	index := NewIndex(src)

	err := index.load()
	if err != nil {
		return nil, fmt.Errorf("index %q data load err: %w", name, err)
	}

	s.indexes[name] = index

	return index, nil
}

func (s *Storage) DeleteIndex(name string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	delete(s.indexes, name)
}

func (s *Storage) GetIndex(name string) (*Index, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	fs, ok := s.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index %q not found", name)
	}

	return fs, nil
}
