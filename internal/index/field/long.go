package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Long struct {
	data map[int64]*roaring.Bitmap
	mtx  sync.RWMutex
}

func NewLong(ctx context.Context) *Long {
	return &Long{
		data: make(map[int64]*roaring.Bitmap),
	}
}

func (f *Long) Type() Type {
	return TypeLong
}

func (f *Long) AddValue(id uint32, value interface{}) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("required int64, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Long) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("required int64, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Long) addValue(id uint32, value int64) {
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
