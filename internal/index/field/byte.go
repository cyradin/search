package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type byteValue struct {
	id    uint32
	value int8
	ready chan struct{}
}

type Byte struct {
	data map[int8]*roaring.Bitmap
	in   chan byteValue
}

func NewByte(ctx context.Context) *Byte {
	result := &Byte{
		data: make(map[int8]*roaring.Bitmap),
	}

	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Byte) Type() Type {
	return TypeByte
}

func (f *Byte) AddValue(id uint32, value interface{}) error {
	return f.addValue(id, value, false)
}

func (f *Byte) AddValueSync(id uint32, value interface{}) error {
	return f.addValue(id, value, true)
}

func (f *Byte) addValue(id uint32, value interface{}, sync bool) error {
	vv, ok := value.(int8)
	if !ok {
		return fmt.Errorf("required int8, got %s", reflect.TypeOf(value))
	}

	var ready chan struct{}
	if sync {
		ready = make(chan struct{})
		defer close(ready)
	}

	f.in <- byteValue{
		id: id, value: vv, ready: ready,
	}

	if sync {
		<-ready
	}

	return nil
}

func (f *Byte) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan byteValue)
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
func (f *Byte) monitorAdd(v byteValue) {
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
