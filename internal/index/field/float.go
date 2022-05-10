package field

import (
	"context"
)

var _ Field = (*Float)(nil)

type Float struct {
	inner *field[float32]
}

func NewFloat(ctx context.Context, src string) (*Float, error) {
	gf, err := newGenericField[float32](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Float{
		inner: gf,
	}, nil
}

func (f *Float) Type() Type {
	return TypeFloat
}

func (f *Float) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Float) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}
