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
	values   *docValues[string]
	raw      *docValues[string]
}

var _ Field = (*Text)(nil)

func newText(analyzer func([]string) []string, scoring *Scoring) *Text {
	return &Text{
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
	}
}

func (f *Text) TermQuery(ctx context.Context, value interface{}) *QueryResult {
	v, err := cast.ToStringE(value)
	if err != nil {
		return newResult(ctx, roaring.New())
	}
	docs := f.values.DocsByValue(v)

	return newResultWithScoring(ctx, docs, f.scoring, WithTokens([]string{v}))
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

		docs := f.values.DocsByValue(v)
		if result == nil {
			result = docs
		} else {
			result.Or(docs)
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

func (f *Text) DeleteDoc(id uint32) {
	if !f.values.ContainsDoc(id) {
		return
	}

	vals := f.values.ValuesByDoc(id)
	if len(vals) == 0 {
		return
	}

	f.values.DeleteDoc(id)
	f.raw.DeleteDoc(id)
	f.scoring.Delete(id)
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

func (f *Text) MinValue() (interface{}, *roaring.Bitmap) {
	return f.values.MinValue()
}

func (f *Text) MaxValue() (interface{}, *roaring.Bitmap) {
	return f.values.MaxValue()
}

func (f *Text) TermAgg(ctx context.Context, docs *roaring.Bitmap, size int) TermAggResult {
	return termAgg(docs, f.values, size)
}

type textData struct {
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
	err = gob.NewEncoder(&buf).Encode(textData{Values: f.values, Scoring: scoringData, Raw: f.raw})

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

	f.values = raw.Values
	f.raw = raw.Raw

	return nil
}
