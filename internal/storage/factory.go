package storage

import (
	"fmt"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/pkg/finisher"
)

type Driver string

const (
	FileDriver Driver = "file"
)

var ErrInvalidDriver = fmt.Errorf("invalid driver")

type Factory struct {
	driver Driver
	config interface{}
}

func NewFactory(driver Driver, config interface{}) (*Factory, error) {
	return &Factory{
		driver: driver,
		config: config,
	}, nil
}

var driver Driver = FileDriver

func SetDriver(d Driver) {
	driver = d
}

func (f *Factory) NewIndexStorage() (Storage[entity.Index], error) {
	var (
		storage Storage[entity.Index]
		err     error
	)

	switch driver {
	case FileDriver:
		config := f.config.(FileConfig)
		storage, err = NewFile[entity.Index](config.PathIndexes())
	default:
		return nil, ErrInvalidDriver
	}

	if err != nil {
		return nil, err
	}

	if s, ok := storage.(finisher.Stoppable); ok {
		finisher.Add(s)
	}

	return storage, nil
}

func (f *Factory) NewIndexSourceStorage(name string) (Storage[entity.DocSource], error) {
	var (
		storage Storage[entity.DocSource]
		err     error
	)

	switch driver {
	case FileDriver:
		config := f.config.(FileConfig)
		storage, err = NewFile[entity.DocSource](config.PathIndexSourceStorage(name))
	}

	if err != nil {
		return nil, err
	}

	if s, ok := storage.(finisher.Stoppable); ok {
		finisher.Add(s)
	}

	return storage, nil
}
