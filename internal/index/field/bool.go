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

func newBool() *Bool {
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

func (f *Bool) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := cast.ToBoolE(value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	return newResult(ctx, f.get(v))
}

func (f *Bool) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	return f.TermQuery(ctx, value)
}

func (f *Bool) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	return newResult(ctx, roaring.New())
}

func (f *Bool) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	if size == 0 {
		return TermAggResult{}
	}

	trueBucket := TermBucket{
		Key:      true,
		DocCount: 0,
	}
	falseBucket := TermBucket{
		Key:      false,
		DocCount: 0,
	}

	for _, id := range docs.ToArray() {
		if f.dataFalse.Contains(id) {
			falseBucket.DocCount++
		}
		if f.dataTrue.Contains(id) {
			trueBucket.DocCount++
		}
	}

	result := TermAggResult{
		Buckets: make([]TermBucket, 0, 2),
	}

	if size == 1 {
		if trueBucket.DocCount > falseBucket.DocCount {
			result.Buckets = append(result.Buckets, trueBucket)
		} else if falseBucket.DocCount > trueBucket.DocCount {
			result.Buckets = append(result.Buckets, falseBucket)
		}
	} else {
		if trueBucket.DocCount > 0 {
			result.Buckets = append(result.Buckets, trueBucket)
		}
		if falseBucket.DocCount > 0 {
			result.Buckets = append(result.Buckets, falseBucket)
		}
	}

	return result
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
