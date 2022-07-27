package field

import (
	"bytes"
	"context"
	"encoding/gob"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

var _ Field = (*Numeric[int32])(nil)

type NumericConstraint interface {
	int8 | int16 | int32 | int64 | uint64 | float32 | float64
}

type Numeric[T NumericConstraint] struct {
	data   map[T]*roaring.Bitmap
	values map[uint32]map[T]struct{}
	list   []T
}

func NewNumeric[T NumericConstraint]() *Numeric[T] {
	return &Numeric[T]{
		data:   make(map[T]*roaring.Bitmap),
		values: make(map[uint32]map[T]struct{}),
	}
}

func (f *Numeric[T]) Type() schema.Type {
	var k T
	var result schema.Type
	switch any(k).(type) {
	case int8:
		result = schema.TypeByte
	case int16:
		result = schema.TypeShort
	case int32:
		result = schema.TypeInteger
	case int64:
		result = schema.TypeLong
	case uint64:
		result = schema.TypeUnsignedLong
	case float32:
		result = schema.TypeFloat
	case float64:
		result = schema.TypeDouble
	}

	return result
}

func (f *Numeric[T]) Add(id uint32, value interface{}) {
	v, err := castE[T](value)
	if err != nil {
		return
	}

	if f.values[id] == nil {
		f.values[id] = make(map[T]struct{})
	}
	f.values[id][v] = struct{}{}

	m, ok := f.data[v]
	if !ok {
		m = roaring.New()
		f.data[v] = m
	}

	m.Add(id)
	f.listAdd(v)
}

func (f *Numeric[T]) Term(ctx context.Context, value interface{}) *Result {
	v, err := castE[T](value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, m.Clone())
}

func (f *Numeric[T]) Match(ctx context.Context, value interface{}) *Result {
	return f.Term(ctx, value)
}

func (f *Numeric[T]) Range(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result {
	if from == nil && to == nil {
		return NewResult(ctx, roaring.New())
	}

	fromIndex := 0
	toIndex := len(f.values) - 1
	if from != nil {
		v, err := castE[T](from)
		if err != nil {
			return NewResult(ctx, roaring.New())
		}

		if incFrom {
			fromIndex = f.findGte(v)
		} else {
			fromIndex = f.findGt(v)
		}
	}

	if to != nil {
		v, err := castE[T](to)
		if err != nil {
			return NewResult(ctx, roaring.New())
		}

		if incTo {
			toIndex = f.findLte(v)
		} else {
			toIndex = f.findLt(v)
		}
	}

	if fromIndex == len(f.values) || toIndex == len(f.values) || fromIndex > toIndex {
		return NewResult(ctx, roaring.New())
	}

	bm := make([]*roaring.Bitmap, 0, toIndex-fromIndex+1)
	for i := fromIndex; i <= toIndex; i++ {
		v, ok := f.data[f.list[i]]
		if !ok {
			continue // @todo broken index
		}

		bm = append(bm, v)
	}

	return NewResult(ctx, roaring.FastOr(bm...))
}

func (f *Numeric[T]) Delete(id uint32) {
	vals, ok := f.values[id]
	if !ok {
		return
	}
	delete(f.values, id)

	for v := range vals {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		m.Remove(id)
		if m.GetCardinality() == 0 {
			delete(f.data, v)
			f.listDel(v)
		}
	}
}

func (f *Numeric[T]) Data(id uint32) []interface{} {
	var result []interface{}

	for v := range f.values[id] {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		if m.Contains(id) {
			result = append(result, v)
		}
	}

	return result
}

type numericData[T NumericConstraint] struct {
	Data   map[T]*roaring.Bitmap
	Values map[uint32]map[T]struct{}
}

func (f *Numeric[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(numericData[T]{Data: f.data, Values: f.values})

	return buf.Bytes(), err
}

func (f *Numeric[T]) UnmarshalBinary(data []byte) error {
	raw := numericData[T]{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.data = raw.Data
	f.values = raw.Values

	return nil
}

func (f *Numeric[T]) listAdd(v T) {
	index := sort.Search(len(f.list), func(i int) bool { return v <= f.list[i] })
	if index == len(f.list) {
		f.list = append(f.list, v)
	} else if f.list[index] != v {
		f.list = append(f.list[:index+1], f.list[index:]...)
		f.list[index] = v
	}
}

func (f *Numeric[T]) findGt(v T) int {
	return sort.Search(len(f.list), func(i int) bool { return f.list[i] > v })
}

func (f *Numeric[T]) findGte(v T) int {
	return sort.Search(len(f.list), func(i int) bool { return f.list[i] >= v })
}

func (f *Numeric[T]) findLt(v T) int {
	return sort.Search(len(f.list), func(i int) bool { return f.list[i] >= v }) - 1
}

func (f *Numeric[T]) findLte(v T) int {
	return sort.Search(len(f.list), func(i int) bool { return f.list[i] > v }) - 1
}

func (f *Numeric[T]) listDel(v T) {
	index := sort.Search(len(f.list), func(i int) bool { return v <= f.list[i] })
	if index == len(f.list) {
		return
	}

	if f.list[index] == v {
		f.list = append(f.list[:index], f.list[index+1:]...)
	}
}
