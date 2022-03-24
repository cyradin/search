package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type UnsignedLong struct {
	data map[uint64]*roaring.Bitmap
	mtx  sync.RWMutex
}

func NewUnsignedLong(ctx context.Context) *UnsignedLong {
	return &UnsignedLong{
		data: make(map[uint64]*roaring.Bitmap),
	}
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
