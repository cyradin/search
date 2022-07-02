package field

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/logger"
	"go.uber.org/zap"
)

const fieldsDir = "fields"
const dirPermissions = 0755
const filePermissions = 0644
const fieldFileExt = ".gob"

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
		for _, index := range result.indexes {
			if err := result.dumpIndex(index); err != nil {
				logger.FromCtx(ctx).Error("field.index.dump.error", logger.ExtractFields(ctx, zap.Error(err))...)
			}
		}
	})

	return result
}

func (s *Storage) AddIndex(name string, sc schema.Schema) (*Index, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, ok := s.indexes[name]; ok {
		return nil, fmt.Errorf("index %q aready initialized", name)
	}

	src := path.Join(s.src, name, fieldsDir)
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, fmt.Errorf("index dir %q create err: %w", src, err)
	}

	index, err := NewIndex(name, sc)
	if err != nil {
		return nil, fmt.Errorf("index %q init err: %w", name, err)
	}

	err = s.loadIndex(index)
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

func (s *Storage) loadIndex(index *Index) error {
	dir := s.indexFieldsDir(index.name)

	return filepath.Walk(dir, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := strings.TrimRight(info.Name(), fieldFileExt)
		f, ok := index.fields[name]
		if !ok {
			return nil
		}

		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("file %q read err: %w", p, err)
		}
		err = f.UnmarshalBinary(data)
		if err != nil {
			return fmt.Errorf("field %q unmarshal err: %w", name, err)
		}

		return nil
	})
}

func (s *Storage) dumpIndex(index *Index) error {
	dir := s.indexFieldsDir(index.name)
	err := os.MkdirAll(dir, dirPermissions)
	if err != nil {
		return fmt.Errorf("dir %q create err: %w", dir, err)
	}

	for name, field := range index.fields {
		src := path.Join(dir, name+fieldFileExt)
		data, err := field.MarshalBinary()
		if err != nil {
			return fmt.Errorf("field %q marshal err: %w", name, err)
		}
		err = os.WriteFile(src, data, filePermissions)
		if err != nil {
			return fmt.Errorf("file %q write err: %w", src, err)
		}
	}

	return nil
}

func (s *Storage) indexDir(name string) string {
	return path.Join(s.src, name)
}

func (s *Storage) indexFieldsDir(name string) string {
	return path.Join(s.indexDir(name), fieldsDir)
}
