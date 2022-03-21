package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type keywordValue struct {
	id    uint32
	value string
	ready chan struct{}
}

type Keyword struct {
	data map[string]*roaring.Bitmap

	in chan keywordValue
}

func NewKeyword(ctx context.Context) *Keyword {
	result := &Keyword{
		data: make(map[string]*roaring.Bitmap),
	}
	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Keyword) Type() Type {
	return TypeKeyword
}

func (f *Keyword) AddValue(id uint32, value interface{}) error {
	vv, ok := value.(string)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}

	f.in <- keywordValue{
		id: id, value: vv,
	}

	return nil
}

func (f *Keyword) AddValueSync(id uint32, value interface{}) error {
	vv, ok := value.(string)
	if !ok {
		return fmt.Errorf("required bool, got %s", reflect.TypeOf(value))
	}

	ready := make(chan struct{})
	defer close(ready)
	f.in <- keywordValue{
		id: id, value: vv, ready: ready,
	}
	<-ready

	return nil
}

func (f *Keyword) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan keywordValue)
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
func (f *Keyword) doAdd(v keywordValue) {
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
