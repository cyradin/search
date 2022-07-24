package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	dataTrue  *roaring.Bitmap
	dataFalse *roaring.Bitmap
}

func NewBool() *Bool {
	return &Bool{
		dataTrue:  roaring.New(),
		dataFalse: roaring.New(),
	}
}

func (f *Bool) Type() schema.Type {
	return schema.TypeBool
}

func (f *Bool) Add(id uint32, value interface{}) {
	v, err := cast.ToBoolE(value)
	if err != nil {
		return
	}

	if v {
		f.dataTrue.Add(id)
	} else {
		f.dataFalse.Add(id)
	}
}

func (f *Bool) Term(ctx context.Context, value interface{}) *Result {
	v, err := cast.ToBoolE(value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, f.get(v))
}

func (f *Bool) GetOr(ctx context.Context, values []interface{}) *Result {
	var result *roaring.Bitmap
	for _, value := range values {
		v, err := cast.ToBoolE(value)
		if err != nil {
			continue
		}
		if result == nil {
			result = f.get(v)
		} else {
			result.Or(f.get(v))
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, result)
}

func (f *Bool) GetAnd(ctx context.Context, values []interface{}) *Result {
	var result *roaring.Bitmap
	for _, value := range values {
		v, err := cast.ToBoolE(value)
		if err != nil {
			continue
		}
		if result == nil {
			result = f.get(v)
		} else {
			result.And(f.get(v))
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, result)
}

func (f *Bool) Delete(id uint32) {
	f.dataTrue.Remove(id)
	f.dataFalse.Remove(id)
}

func (f *Bool) Data(id uint32) []interface{} {
	result := make([]interface{}, 0, 2)

	if f.dataTrue.Contains(id) {
		result = append(result, true)
	}
	if f.dataFalse.Contains(id) {
		result = append(result, false)
	}

	return result
}

func (f *Bool) get(value bool) *roaring.Bitmap {
	if value {
		return f.dataTrue.Clone()
	} else {
		return f.dataFalse.Clone()
	}
}

type boolData struct {
	DataTrue  *roaring.Bitmap
	DataFalse *roaring.Bitmap
}

func (f *Bool) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(boolData{f.dataTrue, f.dataFalse})

	return buf.Bytes(), err
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	raw := boolData{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.dataTrue = raw.DataTrue
	f.dataFalse = raw.DataFalse

	return nil
}
