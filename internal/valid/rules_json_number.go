package valid

import (
	"context"
	"encoding/json"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

func NewErrJsonNumber(ctx context.Context) validation.Error {
	return validation.NewError("validation_json_number", "must be a json.Number").
		SetParams(ErrParams(Path(ctx)))
}

func JsonNumber() validation.RuleWithContextFunc {
	return func(ctx context.Context, value interface{}) error {
		_, ok := value.(json.Number)
		if !ok {
			return NewErrJsonNumber(ctx)
		}

		return nil
	}
}

func JsonNumberInt() validation.RuleWithContextFunc {
	return func(ctx context.Context, value interface{}) error {
		err := JsonNumber()(ctx, value)
		if err != nil {
			return err
		}
		val := value.(json.Number)

		eo := validation.NewError(is.ErrInt.Code(), is.ErrInt.Message()).SetParams(ErrParams(Path(ctx)))
		return is.Int.ErrorObject(eo).Validate(val)
	}
}

func JsonNumberFloat() validation.RuleWithContextFunc {
	return func(ctx context.Context, value interface{}) error {
		err := JsonNumber()(ctx, value)
		if err != nil {
			return err
		}
		val := value.(json.Number)

		eo := validation.NewError(is.ErrInt.Code(), is.ErrInt.Message()).SetParams(ErrParams(Path(ctx)))
		return is.Float.ErrorObject(eo).Validate(val)
	}
}

func JsonNumberIntMin(min int) validation.RuleWithContextFunc {
	return func(ctx context.Context, value interface{}) error {
		err := JsonNumberInt()(ctx, value)
		if err != nil {
			return err
		}
		val := value.(json.Number)
		v, _ := val.Int64()

		eo := validation.NewError(
			validation.ErrMinGreaterThanRequired.Code(),
			validation.ErrMinGreaterThanRequired.Message(),
		).SetParams(ErrParams(Path(ctx)))

		err = validation.Required.ErrorObject(eo).Validate(v)
		if err != nil {
			return err
		}

		return validation.Min(min).ErrorObject(eo).Validate(v)
	}
}
