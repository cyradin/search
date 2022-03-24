package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	data map[bool]*roaring.Bitmap
	mtx  sync.RWMutex
}

func NewBool(ctx context.Context) *Bool {
	return &Bool{
		data: make(map[bool]*roaring.Bitmap),
	}
}

func (f *Bool) Type() Type {
	return TypeBool
}

func (f *Bool) AddValue(id uint32, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Bool) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Bool) addValue(id uint32, value bool) {
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
