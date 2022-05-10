package field

import (
	"context"
)

var _ Field = (*Short)(nil)

type Short struct {
	inner *field[int16]
}

func NewShort(ctx context.Context, src string) (*Short, error) {
	gf, err := newGenericField[int16](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Short{
		inner: gf,
	}, nil
}

func (f *Short) Type() Type {
	return TypeShort
}

func (f *Short) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Short) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}
