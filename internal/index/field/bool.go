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
	values *docValues[bool]
}

func newBool() *Bool {
	return &Bool{
		values: newDocValues[bool](),
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

	f.values.Add(id, v)
}

func (f *Bool) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := cast.ToBoolE(value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	return newResult(ctx, f.values.DocsByValue(v))
}

func (f *Bool) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *Bool) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	return newResult(ctx, roaring.New())
}

func (f *Bool) Delete(id uint32) {
	f.values.DeleteDoc(id)
}

func (f *Bool) Data(id uint32) []interface{} {
	values := f.values.ValuesByDoc(id)
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = v
	}

	return result
}

func (f *Bool) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	return termAgg(docs, f.values, size)
}

type boolData struct {
	Values *docValues[bool]
}

func (f *Bool) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(boolData{Values: f.values})

	return buf.Bytes(), err
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	raw := boolData{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.values = raw.Values

	return nil
}
