package field

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/logger"
	"go.uber.org/zap"
)

const (
	dirPermissions  = 0755
	filePermissions = 0644
	fieldsDir       = "fields"
	fieldFileExt    = ".bin"
)

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
		return nil, errs.Errorf("index %q aready initialized", name)
	}

	src := path.Join(s.src, name, fieldsDir)
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, errs.Errorf("index dir %q create err: %w", src, err)
	}

	index, err := NewIndex(name, sc)
	if err != nil {
		return nil, errs.Errorf("index %q init err: %w", name, err)
	}

	err = s.loadIndex(index)
	if err != nil {
		return nil, errs.Errorf("index %q data load err: %w", name, err)
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
		return nil, errs.Errorf("index %q not found", name)
	}

	return fs, nil
}

func (s *Storage) loadIndex(index *Index) error {
	dir := s.indexFieldsDir(index.name)

	return filepath.Walk(dir, func(src string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := strings.TrimRight(info.Name(), fieldFileExt)
		field, ok := index.fields[name]
		if !ok {
			return nil
		}

		file, err := os.OpenFile(src, os.O_RDONLY|os.O_CREATE, filePermissions)
		if err != nil {
			return errs.Errorf("file %q open err: %w", src, err)
		}

		var buf bytes.Buffer
		_, err = buf.ReadFrom(file)
		if err != nil {
			return errs.Errorf("file %q read err: %w", src, err)
		}

		err = field.UnmarshalBinary(buf.Bytes())
		if err != nil {
			return errs.Errorf("field %q unmarshal err: %w", name, err)
		}

		return nil
	})
}

func (s *Storage) dumpIndex(index *Index) error {
	dir := s.indexFieldsDir(index.name)
	err := os.MkdirAll(dir, dirPermissions)
	if err != nil {
		return errs.Errorf("dir %q create err: %w", dir, err)
	}

	for name, field := range index.fields {
		src := path.Join(dir, name+fieldFileExt)
		file, err := os.OpenFile(src, os.O_WRONLY|os.O_CREATE, filePermissions)
		if err != nil {
			return errs.Errorf("file %q open err: %w", src, err)
		}

		data, err := field.MarshalBinary()
		if err != nil {
			return errs.Errorf("field %q unmarshal err: %w", name, err)
		}

		_, err = bytes.NewBuffer(data).WriteTo(file)
		if err != nil {
			return errs.Errorf("file %q write err: %w", src, err)
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
