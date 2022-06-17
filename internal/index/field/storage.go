package field

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/logger"
	"go.uber.org/zap"
)

const fieldsDir = "fields"
const dirPermissions = 0755
const filePermissions = 0644
const fileExt = ".gob"

type Storage struct {
	src    string
	mtx    sync.RWMutex
	fields map[string]*index
}

type Index interface {
	Add(id uint32, source map[string]interface{})
}

func NewStorage(src string) *Storage {
	result := &Storage{
		fields: make(map[string]*index),
	}

	events.Subscribe(events.NewAppStop(), func(ctx context.Context, e events.Event) {
		result.mtx.Lock()
		defer result.mtx.Unlock()
		for _, f := range result.fields {
			if err := f.dump(); err == nil {
				logger.FromCtx(ctx).Error("field.storage.dump.error", logger.ExtractFields(ctx, zap.Error(err))...)
			}
		}
	})

	return result
}

func (s *Storage) AddIndex(name string, sc schema.Schema) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, ok := s.fields[name]; ok {
		return fmt.Errorf("fields %q aready initialized", name)
	}

	src := path.Join(s.src, name, fieldsDir)
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return fmt.Errorf("fields dir %q create err: %w", src, err)
	}

	storage, err := NewIndex(src, sc)
	if err != nil {
		return fmt.Errorf("fields %q init err: %w", name, err)
	}

	s.fields[name] = storage

	return nil
}

func (s *Storage) DeleteIndex(name string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	delete(s.fields, name)

	return nil
}

func (s *Storage) GetIndex(name string) (*index, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	fs, ok := s.fields[name]
	if !ok {
		return nil, fmt.Errorf("fields %q not found", name)
	}

	return fs, nil
}
