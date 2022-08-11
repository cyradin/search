package valid

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func NewErrString(ctx context.Context, msg string) validation.Error {
	return validation.NewError("validation_string", msg).
		SetParams(ErrParams(Path(ctx)))
}

func String() validation.RuleWithContextFunc {
	return func(ctx context.Context, value interface{}) error {
		_, err := validation.EnsureString(value)
		if err != nil {
			return NewErrString(ctx, err.Error())
		}
		return nil
	}
}
