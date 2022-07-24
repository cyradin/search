package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

const (
	AllField = "_all"
)

var _ Field = (*All)(nil)

// All contains every document in the index.
// This field is necessary to execute queris like { "bool": {}} and {"match_all":{}}
type All struct {
	data *roaring.Bitmap
}

func NewAll() *All {
	return &All{
		data: roaring.New(),
	}
}

func (f *All) Type() schema.Type {
	return schema.TypeAll
}

func (f *All) Add(id uint32, value interface{}) {
	f.data.Add(id)
}

func (f *All) Term(ctx context.Context, value interface{}) *Result {
	return NewResult(ctx, f.data.Clone())
}

func (f *All) GetOr(ctx context.Context, values []interface{}) *Result {
	return f.Term(ctx, true)
}

func (f *All) GetAnd(ctx context.Context, values []interface{}) *Result {
	return f.Term(ctx, true)
}

func (f *All) Delete(id uint32) {
	f.data.Remove(id)
}

func (f *All) Data(id uint32) []interface{} {
	return []interface{}{f.data.Contains(id)}
}

func (f *All) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f.data)

	return buf.Bytes(), err
}

func (f *All) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	return gob.NewDecoder(buf).Decode(&f.data)
}
