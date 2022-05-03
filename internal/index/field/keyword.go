package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type Keyword struct {
	data map[string]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewKeyword(ctx context.Context, src string) (*Keyword, error) {
	data, err := readField[string](src)
	if err != nil {
		return nil, err
	}

	result := &Keyword{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
}

func (f *Keyword) Type() Type {
	return TypeKeyword
}

func (f *Keyword) AddValue(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("required string, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Keyword) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("required string, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Keyword) addValue(id uint32, value string) {
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

func (f *Keyword) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
