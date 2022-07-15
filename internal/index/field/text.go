package field

import (
	"bytes"
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
var _ FTS = (*Text)(nil)

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

	for _, vv := range f.analyzer([]string{val}) {
		f.inner.Add(id, vv)
	}
}

func (f *Text) Get(value interface{}) *roaring.Bitmap {
	val, err := castE[string](value)
	if err != nil {
		return roaring.New()
	}

	return f.inner.Get(val)
}

func (f *Text) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(castSlice[string](values))
}

func (f *Text) GetAnd(values []interface{}) *roaring.Bitmap {
	return f.inner.GetAnd(castSlice[string](values))
}

func (f *Text) GetOrAnalyzed(value interface{}) (*roaring.Bitmap, map[uint32]float64) {
	v, err := castE[string](value)
	if err != nil {
		return roaring.New(), make(map[uint32]float64)
	}
	if v == "" {
		return roaring.New(), make(map[uint32]float64)
	}

	tokens := f.analyzer([]string{v})
	bm := f.inner.GetOr(tokens)

	return bm, f.scores(bm, tokens)
}

func (f *Text) GetAndAnalyzed(value interface{}) (*roaring.Bitmap, map[uint32]float64) {
	v, err := castE[string](value)
	if err != nil {
		return roaring.New(), make(map[uint32]float64)
	}
	if v == "" {
		return roaring.New(), make(map[uint32]float64)
	}

	tokens := f.analyzer([]string{v})
	bm := f.inner.GetAnd(tokens)

	return bm, f.scores(bm, tokens)
}

func (f *Text) scores(bm *roaring.Bitmap, tokens []string) map[uint32]float64 {
	result := make(map[uint32]float64)
	bm.Iterate(func(x uint32) bool {
		score := 0.0
		for _, t := range tokens {
			score += f.scoring.BM25(x, 2, 0.75, t)
		}
		result[x] = score
		return true
	})
	return result
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
