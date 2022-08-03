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
	values   *docValues[string]
	raw      *docValues[string]
}

var _ Field = (*Text)(nil)

func newText(analyzer func([]string) []string, scoring *Scoring) *Text {
	return &Text{
		data:     make(map[string]*roaring.Bitmap),
		values:   newDocValues[string](),
		raw:      newDocValues[string](),
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

	f.raw.Add(id, v)

	terms := f.analyzer([]string{v})
	f.scoring.Add(id, terms)

	for _, vv := range terms {
		f.values.Add(id, vv)
		m, ok := f.data[vv]
		if !ok {
			m = roaring.New()
			f.data[vv] = m
		}
		m.Add(id)
	}
}

func (f *Text) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := cast.ToStringE(value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return newResult(ctx, roaring.New())
	}

	return newResultWithScoring(ctx, m.Clone(), f.scoring, WithTokens([]string{v}))
}

func (f *Text) MatchQuery(ctx context.Context, value interface{}) *QueryResult {
	val, err := castE[string](value)
	if err != nil {
		return newResult(ctx, roaring.New())
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
		return newResult(ctx, roaring.New())
	}

	return newResultWithScoring(ctx, result, f.scoring, WithTokens(tokens))
}

func (f *Text) RangeQuery(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *QueryResult {
	return newResult(ctx, roaring.New())
}

func (f *Text) Delete(id uint32) {
	if !f.values.ContainsDoc(id) {
		return
	}

	vals := f.values.ValuesByDoc(id)
	if len(vals) == 0 {
		return
	}

	f.values.DeleteDoc(id)
	f.raw.DeleteDoc(id)

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
	result := make([]interface{}, 0)

	if f.raw.ContainsDoc(id) {
		for _, v := range f.raw.ValuesByDoc(id) {
			result = append(result, v)
		}
	}

	return result
}

type textData struct {
	Data    map[string]*roaring.Bitmap
	Values  *docValues[string]
	Raw     *docValues[string]
	Scoring []byte
}

func (f *Text) MarshalBinary() ([]byte, error) {
	scoringData, err := f.scoring.MarshalBinary()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(textData{Data: f.data, Values: f.values, Scoring: scoringData, Raw: f.raw})

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
	f.raw = raw.Raw

	return nil
}
