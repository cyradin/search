package field

import (
	"context"
	"encoding"
	"sync"

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
	// Term get field value
	Term(ctx context.Context, value interface{}) *Result
	// Match get field analyzed value
	Match(ctx context.Context, value interface{}) *Result
	// Range get values from .. to ...
	Range(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result
	// Delete document field values
	Delete(id uint32)
	// Data get stored field values
	Data(id uint32) []interface{}
}

type Sync struct {
	mtx   sync.RWMutex
	field Field
}

func NewSync(field Field) *Sync {
	return &Sync{field: field}
}

func (f *Sync) Type() schema.Type {
	return f.field.Type()
}

func (f *Sync) Add(id uint32, value interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field.Add(id, value)
}

func (f *Sync) Term(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Term(ctx, value)
}

func (f *Sync) Match(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Term(ctx, value)
}

func (f *Sync) Range(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Range(ctx, from, to, incFrom, incTo)
}

func (f *Sync) Delete(id uint32) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field.Delete(id)
}

func (f *Sync) Data(id uint32) []interface{} {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Data(id)
}

func (f *Sync) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.field.MarshalBinary()
}

func (f *Sync) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.field.UnmarshalBinary(data)
}

func New(t schema.Type, opts ...FieldOpts) (Field, error) {
	var field Field

	switch t {
	case schema.TypeAll:
		field = NewAll()
	case schema.TypeBool:
		field = NewBool()
	case schema.TypeKeyword:
		field = NewKeyword()
	case schema.TypeText:
		if len(opts) == 0 || opts[0].Scoring == nil {
			return nil, errs.Errorf("field scoring data required, but not provided")
		}
		field = NewText(opts[0].Analyzer, opts[0].Scoring)
	// @todo implement slice type
	// case schema.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice()
	// @todo implement map type
	// case schema.TypeNap:
	// 	i.fields[f.Name] = field.NewMap()
	case schema.TypeUnsignedLong:
		field = NewNumeric[uint64]()
	case schema.TypeLong:
		field = NewNumeric[int64]()
	case schema.TypeInteger:
		field = NewNumeric[int32]()
	case schema.TypeShort:
		field = NewNumeric[int16]()
	case schema.TypeByte:
		field = NewNumeric[int8]()
	case schema.TypeDouble:
		field = NewNumeric[float64]()
	case schema.TypeFloat:
		field = NewNumeric[float32]()
	default:
		return nil, errs.Errorf("invalid field type %q", t)
	}

	return NewSync(field), nil
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
