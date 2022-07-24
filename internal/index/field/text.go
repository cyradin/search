package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type Text struct {
	analyzer func([]string) []string
	scoring  *Scoring
	data     map[string]*roaring.Bitmap
	values   map[uint32][]string
}

var _ Field = (*Text)(nil)

func NewText(analyzer func([]string) []string, scoring *Scoring) *Text {
	return &Text{
		data:     make(map[string]*roaring.Bitmap),
		values:   make(map[uint32][]string),
		analyzer: analyzer,
		scoring:  scoring,
	}
}

func (f *Text) Type() schema.Type {
	return schema.TypeText
}

func (f *Text) Add(id uint32, value interface{}) {
	v, err := cast.ToStringE(value)
	if err != nil {
		return
	}

	terms := f.analyzer([]string{v})
	f.scoring.Add(id, terms)

	for _, vv := range terms {
		f.values[id] = append(f.values[id], vv)

		m, ok := f.data[vv]
		if !ok {
			m = roaring.New()
			f.data[vv] = m
		}
		m.Add(id)
	}
}

func (f *Text) Term(ctx context.Context, value interface{}) *Result {
	v, err := cast.ToStringE(value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return NewResult(ctx, roaring.New())
	}

	return NewResultWithScoring(ctx, m.Clone(), f.scoring, WithTokens([]string{v}))
}

func (f *Text) Match(ctx context.Context, value interface{}) *Result {
	val, err := castE[string](value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}
	tokens := f.analyzer([]string{val})

	var result *roaring.Bitmap
	for _, value := range tokens {
		v, err := cast.ToStringE(value)
		if err != nil {
			continue
		}

		m, ok := f.data[v]
		if !ok {
			continue
		}

		if result == nil {
			result = m.Clone()
		} else {
			result.Or(m)
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResultWithScoring(ctx, result, f.scoring, WithTokens(tokens))
}

func (f *Text) GetOr(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, roaring.New()) // no implemented (yet?)
}

func (f *Text) GetAnd(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, roaring.New()) // no implemented (yet?)
}

func (f *Text) Delete(id uint32) {
	vals, ok := f.values[id]
	if !ok {
		return
	}
	delete(f.values, id)

	for _, v := range vals {
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

func (f *Text) Data(id uint32) []interface{} {
	var result []interface{}

	for _, v := range f.values[id] {
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

type textData struct {
	Data    map[string]*roaring.Bitmap
	Values  map[uint32][]string
	Scoring []byte
}

func (f *Text) MarshalBinary() ([]byte, error) {
	scoringData, err := f.scoring.MarshalBinary()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(textData{Data: f.data, Values: f.values, Scoring: scoringData})

	return buf.Bytes(), nil
}

func (f *Text) UnmarshalBinary(data []byte) error {
	raw := textData{}
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&raw)
	if err != nil {
		return err
	}

	err = f.scoring.UnmarshalBinary(raw.Scoring)
	if err != nil {
		return err
	}

	f.data = raw.Data
	f.values = raw.Values

	return nil
}
