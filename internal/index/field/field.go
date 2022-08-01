package field

import (
	"context"
	"encoding"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type FieldOpts struct {
	Analyzer func([]string) []string
	Scoring  *Scoring
}

type Field interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	// Type returns field type
	Type() schema.Type
	// Add add document field value
	Add(id uint32, value interface{})
	// TermQuery get documents by field value
	TermQuery(ctx context.Context, value interface{}) *QueryResult
	// MatchQuery get documents by field analyzed value
	MatchQuery(ctx context.Context, value interface{}) *QueryResult
	// RangeQuery get documents by values from .. to ...
	RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult
	// TermAgg get doc counts by every available value
	TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult
	// Delete document field values
	Delete(id uint32)
	// Data get stored field values
	Data(id uint32) []interface{}
}

func New(t schema.Type, opts ...FieldOpts) (Field, error) {
	var field Field

	switch t {
	case schema.TypeAll:
		field = newAll()
	case schema.TypeBool:
		field = newBool()
	case schema.TypeKeyword:
		field = newKeyword()
	case schema.TypeText:
		if len(opts) == 0 || opts[0].Scoring == nil {
			return nil, errs.Errorf("field scoring data required, but not provided")
		}
		field = newText(opts[0].Analyzer, opts[0].Scoring)
	// @todo implement slice type
	// case schema.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice()
	// @todo implement map type
	// case schema.TypeNap:
	// 	i.fields[f.Name] = field.NewMap()
	case schema.TypeUnsignedLong:
		field = newNumeric[uint64]()
	case schema.TypeLong:
		field = newNumeric[int64]()
	case schema.TypeInteger:
		field = newNumeric[int32]()
	case schema.TypeShort:
		field = newNumeric[int16]()
	case schema.TypeByte:
		field = newNumeric[int8]()
	case schema.TypeDouble:
		field = newNumeric[float64]()
	case schema.TypeFloat:
		field = newNumeric[float32]()
	default:
		return nil, errs.Errorf("invalid field type %q", t)
	}

	return NewSyncMtx(field), nil
}

func castSlice[T comparable](values []interface{}) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		v, err := castE[T](value)
		if err != nil {
			continue
		}
		result = append(result, v)
	}
	return result
}

func castE[T comparable](value interface{}) (T, error) {
	var (
		k   T
		val interface{}
		err error
	)

	switch any(k).(type) {
	case bool:
		val, err = cast.ToBoolE(value)
	case int8:
		val, err = cast.ToInt8E(value)
	case int16:
		val, err = cast.ToInt16E(value)
	case int32:
		val, err = cast.ToInt32E(value)
	case int64:
		val, err = cast.ToInt64E(value)
	case uint64:
		val, err = cast.ToUint64E(value)
	case float32:
		val, err = cast.ToFloat32E(value)
	case float64:
		val, err = cast.ToFloat64E(value)
	case string:
		val, err = cast.ToStringE(value)
	}

	if err != nil {
		return k, err
	}

	return val.(T), err
}
