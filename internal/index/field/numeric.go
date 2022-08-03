package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

var _ Field = (*Numeric[int32])(nil)

type NumericConstraint interface {
	int8 | int16 | int32 | int64 | uint64 | float32 | float64
}

type Numeric[T NumericConstraint] struct {
	data   map[T]*roaring.Bitmap
	values *docValues[T]
}

func newNumeric[T NumericConstraint]() *Numeric[T] {
	return &Numeric[T]{
		data:   make(map[T]*roaring.Bitmap),
		values: newDocValues[T](),
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

	f.values.Add(id, v)

	m, ok := f.data[v]
	if !ok {
		m = roaring.New()
		f.data[v] = m
	}
	m.Add(id)
}

func (f *Numeric[T]) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := castE[T](value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return newResult(ctx, roaring.New())
	}

	return newResult(ctx, m.Clone())
}

func (f *Numeric[T]) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *Numeric[T]) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	if from == nil && to == nil {
		return newResult(ctx, roaring.New())
	}

	var vFrom, vTo *T
	if from != nil {
		v, err := castE[T](from)
		if err != nil {
			return newResult(ctx, roaring.New())
		}
		vFrom = &v
	}
	if to != nil {
		v, err := castE[T](to)
		if err != nil {
			return newResult(ctx, roaring.New())
		}
		vTo = &v
	}

	cardinality := f.values.Cardinality()
	fromIndex := 0
	toIndex := cardinality - 1
	if from != nil {
		if incFrom {
			fromIndex = f.values.FindGte(*vFrom)
		} else {
			fromIndex = f.values.FindGt(*vFrom)
		}
	}

	if to != nil {
		if incTo {
			toIndex = f.values.FindLte(*vTo)
		} else {
			toIndex = f.values.FindLt(*vTo)
		}
	}

	if fromIndex == cardinality || toIndex == cardinality || fromIndex > toIndex {
		return newResult(ctx, roaring.New())
	}

	bm := make([]*roaring.Bitmap, 0, toIndex-fromIndex+1)
	for i := fromIndex; i <= toIndex; i++ {
		v := f.values.DocsByIndex(i)
		bm = append(bm, v)
	}

	return newResult(ctx, roaring.FastOr(bm...))
}

func (f *Numeric[T]) Delete(id uint32) {
	vals := f.values.ValuesByDoc(id)
	for _, v := range vals {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		m.Remove(id)
		if m.GetCardinality() == 0 {
			delete(f.data, v)
		}
	}
	f.values.DeleteDoc(id)
}

func (f *Numeric[T]) Data(id uint32) []interface{} {
	var result []interface{}

	for _, v := range f.values.ValuesByDoc(id) {
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
	Values *docValues[T]
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
