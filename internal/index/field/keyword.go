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
	data   map[string]*roaring.Bitmap
	values map[uint32]map[string]struct{}
}

func newKeyword() *Keyword {
	return &Keyword{
		data:   make(map[string]*roaring.Bitmap),
		values: make(map[uint32]map[string]struct{}),
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

	if f.values[id] == nil {
		f.values[id] = make(map[string]struct{})
	}
	f.values[id][v] = struct{}{}

	m, ok := f.data[v]
	if !ok {
		m = roaring.New()
		f.data[v] = m
	}
	m.Add(id)
}

func (f *Keyword) TermQuery(ctx context.Context, value interface{}) *Result {
	v, err := cast.ToStringE(value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, m.Clone())
}

func (f *Keyword) MatchQuery(ctx context.Context, value interface{}) *Result {
	return f.TermQuery(ctx, value)
}

func (f *Keyword) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result {
	return NewResult(ctx, roaring.New())
}

func (f *Keyword) Delete(id uint32) {
	vals, ok := f.values[id]
	if !ok {
		return
	}
	delete(f.values, id)

	for v := range vals {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		m.Remove(id)
		if m.GetCardinality() == 0 {
			delete(f.data, v)
		}
	}
}

func (f *Keyword) Data(id uint32) []interface{} {
	var result []interface{}

	for v := range f.values[id] {
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

type keywordData struct {
	Data   map[string]*roaring.Bitmap
	Values map[uint32]map[string]struct{}
}

func (f *Keyword) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(keywordData{Data: f.data, Values: f.values})

	return buf.Bytes(), err
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	raw := keywordData{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.data = raw.Data
	f.values = raw.Values

	return nil
}
