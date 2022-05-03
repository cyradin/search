package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type UnsignedLong struct {
	data map[uint64]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewUnsignedLong(ctx context.Context, src string) (*UnsignedLong, error) {
	data, err := readField[uint64](src)
	if err != nil {
		return nil, err
	}

	result := &UnsignedLong{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
}

func (f *UnsignedLong) Type() Type {
	return TypeUnsignedLong
}

func (f *UnsignedLong) AddValue(id uint32, value interface{}) error {
	v, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("required uint64, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *UnsignedLong) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("required uint64, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *UnsignedLong) addValue(id uint32, value uint64) {
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

func (f *UnsignedLong) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
