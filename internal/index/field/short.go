package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type Short struct {
	data map[int16]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewShort(ctx context.Context, src string) (*Short, error) {
	data, err := readField[int16](src)
	if err != nil {
		return nil, err
	}

	result := &Short{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
}

func (f *Short) Type() Type {
	return TypeShort
}

func (f *Short) AddValue(id uint32, value interface{}) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("required int16, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Short) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("required int16, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Short) addValue(id uint32, value int16) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	m, ok := f.data[value]
	if !ok {
		m = roaring.New()
		f.data[value] = m
	}

	m.Add(id)

	return
}

func (f *Short) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
