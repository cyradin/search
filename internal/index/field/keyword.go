package field

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	mtx    sync.RWMutex
	data   map[string]*roaring.Bitmap
	values map[uint32][]string
}

func NewKeyword() *Keyword {
	return &Keyword{
		data:   make(map[string]*roaring.Bitmap),
		values: make(map[uint32][]string),
	}
}

func (f *Keyword) Type() schema.Type {
	return schema.TypeKeyword
}

func (f *Keyword) Add(id uint32, value interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	v, err := cast.ToStringE(value)
	if err != nil {
		return
	}

	f.values[id] = append(f.values[id], v)

	m, ok := f.data[v]
	if !ok {
		m = roaring.New()
		f.data[v] = m
	}
	m.Add(id)
}

func (f *Keyword) Get(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

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

func (f *Keyword) GetOr(ctx context.Context, values []interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var result *roaring.Bitmap
	for _, value := range values {
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

	return NewResult(ctx, result)
}

func (f *Keyword) GetAnd(ctx context.Context, values []interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var result *roaring.Bitmap
	for _, value := range values {
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
			result.And(m)
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, result)
}

func (f *Keyword) Delete(id uint32) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

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

type keywordData struct {
	Data   map[string]*roaring.Bitmap
	Values map[uint32][]string
}

func (f *Keyword) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(keywordData{Data: f.data, Values: f.values})

	return buf.Bytes(), err
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

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
