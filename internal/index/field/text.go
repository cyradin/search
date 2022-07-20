package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

type Text struct {
	analyzer func([]string) []string
	scoring  *Scoring
	inner    *field[string]
}

var _ Field = (*Text)(nil)

func NewText(analyzer func([]string) []string, scoring *Scoring) *Text {
	gf := newField[string]()

	return &Text{
		inner:    gf,
		analyzer: analyzer,
		scoring:  scoring,
	}
}

func (f *Text) Type() schema.Type {
	return schema.TypeText
}

func (f *Text) Add(id uint32, value interface{}) {
	val, err := castE[string](value)
	if err != nil {
		return
	}

	terms := f.analyzer([]string{val})
	f.scoring.Add(id, terms)

	for _, vv := range terms {
		f.inner.Add(id, vv)
	}
}

func (f *Text) Get(ctx context.Context, value interface{}) *Result {
	val, err := castE[string](value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}
	tokens := f.analyzer([]string{val})

	return NewResultWithScoring(ctx, f.inner.GetOr(tokens), f.scoring, WithTokens(tokens))
}

func (f *Text) GetOr(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, roaring.New()) // no implemented (yet?)
}

func (f *Text) GetAnd(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, roaring.New()) // no implemented (yet?)
}

type textData struct {
	Field   []byte
	Scoring []byte
}

func (f *Text) MarshalBinary() ([]byte, error) {
	fieldData, err := f.inner.MarshalBinary()
	if err != nil {
		return nil, err
	}
	scoringData, err := f.scoring.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	err = gob.NewEncoder(buf).Encode(textData{Field: fieldData, Scoring: scoringData})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *Text) UnmarshalBinary(data []byte) error {
	var textData textData
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&textData)
	if err != nil {
		return err
	}

	err = f.inner.UnmarshalBinary(textData.Field)
	if err != nil {
		return err
	}
	err = f.scoring.UnmarshalBinary(textData.Scoring)
	if err != nil {
		return err
	}

	return nil
}

func sliceToInterfaceSlice[T comparable](data []T) []interface{} {
	result := make([]interface{}, len(data))
	for i, v := range data {
		result[i] = v
	}
	return result
}
