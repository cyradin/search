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

func newAll() *All {
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

func (f *All) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	return newResult(ctx, f.data.Clone())
}

func (f *All) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *All) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	return newResult(ctx, roaring.New())
}

func (f *All) Delete(id uint32) {
	f.data.Remove(id)
}

func (f *All) Data(id uint32) []interface{} {
	return []interface{}{f.data.Contains(id)}
}

func (f *All) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	return TermAggResult{
		Buckets: []TermBucket{},
	}
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
