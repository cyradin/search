package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	values *docValues[string]
}

func newKeyword() *Keyword {
	return &Keyword{
		values: newDocValues[string](),
	}
}

func (f *Keyword) Type() schema.Type {
	return schema.TypeKeyword
}

func (f *Keyword) Add(id uint32, value interface{}) {
	v, err := cast.ToStringE(value)
	if err != nil {
		return
	}

	f.values.Add(id, v)
}

func (f *Keyword) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := cast.ToStringE(value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	return newResult(ctx, f.values.DocsByValue(v))
}

func (f *Keyword) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *Keyword) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	return newResult(ctx, roaring.New())
}

func (f *Keyword) Delete(id uint32) {
	f.values.DeleteDoc(id)
}

func (f *Keyword) Data(id uint32) []interface{} {
	values := f.values.ValuesByDoc(id)
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = v
	}

	return result
}

func (f *Keyword) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	return termAgg(docs, f.values, size)
}

func (f *Keyword) RangeAgg(ctx context.Context, docs *roaring.Bitmap, ranges []Range) RangeAggResult {
	buckets := make([]RangeBucket, len(ranges))
	for i, r := range ranges {
		buckets[i] = RangeBucket{
			Key:  r.Key,
			From: r.From,
			To:   r.To,
			Docs: roaring.New(),
		}
	}

	return RangeAggResult{
		Buckets: buckets,
	}
}

type keywordData struct {
	Values *docValues[string]
}

func (f *Keyword) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(keywordData{Values: f.values})

	return buf.Bytes(), err
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	raw := keywordData{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.values = raw.Values

	return nil
}
