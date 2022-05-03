package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type Byte struct {
	data map[int8]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewByte(ctx context.Context, src string) (*Byte, error) {
	data, err := readField[int8](src)
	if err != nil {
		return nil, err
	}

	result := &Byte{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
}

func (f *Byte) Type() Type {
	return TypeByte
}

func (f *Byte) AddValue(id uint32, value interface{}) error {
	v, ok := value.(int8)
	if !ok {
		return fmt.Errorf("required int8, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Byte) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(int8)
	if !ok {
		return fmt.Errorf("required int8, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Byte) addValue(id uint32, value int8) {
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

func (f *Byte) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
