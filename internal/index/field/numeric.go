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
	values *docValues[T]
}

func newNumeric[T NumericConstraint]() *Numeric[T] {
	return &Numeric[T]{
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
}

func (f *Numeric[T]) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := castE[T](value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	return newResult(ctx, f.values.DocsByValue(v))
}

func (f *Numeric[T]) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *Numeric[T]) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	if from == nil && to == nil {
		return newResult(ctx, roaring.New())
	}

	opts := make([]ResultOpt, 0, 2)
	var vFrom, vTo *T
	if from != nil {
		v, err := castE[T](from)
		if err != nil {
			return newResult(ctx, roaring.New())
		}
		vFrom = &v
		opts = append(opts, WithFrom(v))
	}
	if to != nil {
		v, err := castE[T](to)
		if err != nil {
			return newResult(ctx, roaring.New())
		}
		vTo = &v
		opts = append(opts, WithTo(v))
	}

	return newResult(ctx, rangeQuery(ctx, f.values, vFrom, vTo, incFrom, incTo), opts...)
}

func (f *Numeric[T]) DeleteDoc(id uint32) {
	f.values.DeleteDoc(id)
}

func (f *Numeric[T]) Data(id uint32) []interface{} {
	values := f.values.ValuesByDoc(id)
	result := make([]interface{}, len(values))
	for i, v := range f.values.ValuesByDoc(id) {
		result[i] = v
	}

	return result
}

func (f *Numeric[T]) MinValue() (interface{}, *roaring.Bitmap) {
	return f.values.MinValue()
}

func (f *Numeric[T]) MaxValue() (interface{}, *roaring.Bitmap) {
	return f.values.MaxValue()
}

func (f *Numeric[T]) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	return termAgg(docs, f.values, size)
}

type numericData[T NumericConstraint] struct {
	Values *docValues[T]
}

func (f *Numeric[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(numericData[T]{Values: f.values})

	return buf.Bytes(), err
}

func (f *Numeric[T]) UnmarshalBinary(data []byte) error {
	raw := numericData[T]{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.values = raw.Values

	return nil
}

func rangeQuery[T NumericConstraint](ctx context.Context, data *docValues[T], from *T, to *T, incFrom, incTo bool) *roaring.Bitmap {
	if from == nil && to == nil {
		return roaring.New()
	}

	cardinality := data.Cardinality()
	fromIndex := 0
	toIndex := cardinality - 1
	if from != nil {
		if incFrom {
			fromIndex = data.FindGte(*from)
		} else {
			fromIndex = data.FindGt(*from)
		}
	}

	if to != nil {
		if incTo {
			toIndex = data.FindLte(*to)
		} else {
			toIndex = data.FindLt(*to)
		}
	}

	if fromIndex == cardinality || toIndex == cardinality || fromIndex > toIndex {
		return roaring.New()
	}

	bm := make([]*roaring.Bitmap, 0, toIndex-fromIndex+1)
	for i := fromIndex; i <= toIndex; i++ {
		v := data.DocsByIndex(i)
		bm = append(bm, v)
	}

	return roaring.FastOr(bm...)
}
