package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

var _ Field = (*Bool)(nil)

type boolValue struct {
	id    uint32
	value bool
	ready chan struct{}
}

type Bool struct {
	data map[bool]*roaring.Bitmap

	in chan boolValue
}

func NewBool(ctx context.Context) *Bool {
	result := &Bool{
		data: make(map[bool]*roaring.Bitmap),
	}
	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Bool) Type() Type {
	return TypeBool
}

func (f *Bool) AddValue(id uint32, value interface{}) error {
	vv, ok := value.(bool)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}

	f.in <- boolValue{
		id: id, value: vv,
	}

	return nil
}

func (f *Bool) AddValueSync(id uint32, value interface{}) error {
	vv, ok := value.(bool)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}

	ready := make(chan struct{})
	defer close(ready)
	f.in <- boolValue{
		id: id, value: vv, ready: ready,
	}
	<-ready

	return nil
}

func (f *Bool) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan boolValue)
		defer close(f.in)
		ready <- struct{}{}

		for {
			select {
			case v := <-f.in:
				f.doAdd(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

// doAdd add value to index field. Call only from monitor() method
func (f *Bool) doAdd(v boolValue) {
	m, ok := f.data[v.value]
	if !ok {
		m = roaring.New()
		f.data[v.value] = m
	}

	m.Add(v.id)

	if v.ready != nil {
		v.ready <- struct{}{}
	}
}
