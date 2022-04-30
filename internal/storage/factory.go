package storage

import (
	"fmt"

	"github.com/cyradin/search/internal/entity"
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
	}, nil
}

var driver Driver = FileDriver

func SetDriver(d Driver) {
	driver = d
}

func (f *Factory) NewIndexStorage() (Storage[entity.Index], error) {
	switch driver {
	case FileDriver:
		config := f.config.(FileConfig)
		return NewFile[entity.Index](config.PathIndexes())
	}

	return nil, ErrInvalidDriver
}

func (f *Factory) NewIndexSourceStorage(name string) (Storage[entity.Index], error) {
	switch driver {
	case FileDriver:
		config := f.config.(FileConfig)
		return NewFile[entity.Index](config.PathIndexSourceStorage(name))
	}

	return nil, ErrInvalidDriver
}
