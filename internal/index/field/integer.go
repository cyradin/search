package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type Integer struct {
	data map[int32]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewInteger(ctx context.Context, src string) (*Integer, error) {
	data, err := readField[int32](src)
	if err != nil {
		return nil, err
	}

	result := &Integer{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
}

func (f *Integer) Type() Type {
	return TypeInteger
}

func (f *Integer) AddValue(id uint32, value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("required int32, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Integer) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("required int32, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Integer) addValue(id uint32, value int32) {
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

func (f *Integer) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
