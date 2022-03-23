package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type longValue struct {
	id    uint32
	value int64
	ready chan struct{}
}

type Long struct {
	data map[int64]*roaring.Bitmap
	in   chan longValue
}

func NewLong(ctx context.Context) *Long {
	result := &Long{
		data: make(map[int64]*roaring.Bitmap),
	}

	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Long) Type() Type {
	return TypeLong
}

func (f *Long) AddValue(id uint32, value interface{}) error {
	return f.addValue(id, value, false)
}

func (f *Long) AddValueSync(id uint32, value interface{}) error {
	return f.addValue(id, value, true)
}

func (f *Long) addValue(id uint32, value interface{}, sync bool) error {
	vv, ok := value.(int64)
	if !ok {
		return fmt.Errorf("required int64, got %s", reflect.TypeOf(value))
	}

	var ready chan struct{}
	if sync {
		ready = make(chan struct{})
		defer close(ready)
	}

	f.in <- longValue{
		id: id, value: vv, ready: ready,
	}

	if sync {
		<-ready
	}

	return nil
}

func (f *Long) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan longValue)
		defer close(f.in)
		ready <- struct{}{}

		for {
			select {
			case v := <-f.in:
				f.monitorAdd(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

// monitorAdd add value to index field. Call only from monitor() method
func (f *Long) monitorAdd(v longValue) {
	var m *roaring.Bitmap
	var ok bool

	m, ok = f.data[v.value]
	if !ok {
		m = roaring.New()
		f.data[v.value] = m
	}

	m.Add(v.id)

	if v.ready != nil {
		v.ready <- struct{}{}
	}
}
