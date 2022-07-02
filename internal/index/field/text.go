package field

import (
	"bytes"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type Text struct {
	analyzer func([]string) []string
	scoring  *Scoring
	inner    *field[string]
}

var _ Field = (*Text)(nil)

func NewText(analyzer func([]string) []string, scoring *Scoring) *Text {
	gf := newField[string](cast.ToStringE)

	return &Text{
		inner:    gf,
		analyzer: analyzer,
		scoring:  scoring,
	}
}

func (f *Text) Type() schema.Type {
	return schema.TypeText
}

func (f *Text) AddValue(id uint32, value interface{}) {
	val, err := f.inner.transform(value)
	if err != nil {
		return
	}

	for _, vv := range f.analyzer([]string{val}) {
		f.inner.AddValue(id, vv)
	}
}

func (f *Text) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Text) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Text) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
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
