package errs

import (
	"context"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const PathParam = "path"

func Params(path string) map[string]interface{} {
	return map[string]interface{}{
		PathParam: path,
	}
}

func Required(ctx context.Context) validation.Error {
	return validation.NewError("validation_required", "cannot be blank").
		SetParams(Params(Path(ctx)))
}

func SingleKeyRequired(ctx context.Context) validation.Error {
	return validation.NewError("validation_length_invalid", "the length must be exactly 1").
		SetParams(Params(Path(ctx)))
}

func ArrayRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_array_required", "must be an array").
		SetParams(Params(PathJoin(Path(ctx), key)))
}

func ObjectRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_object_required", "must be an object").
		SetParams(Params(PathJoin(Path(ctx), key)))
}

func UnknownValue(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_unknown_value", "unknown value").
		SetParams(Params(PathJoin(Path(ctx), key)))
}

func PathJoin(path string, parts ...string) string {
	if len(parts) > 0 {
		parts = append([]string{path}, parts...)
		path = strings.Join(parts, ".")
	}

	return path
}

func WithPath(ctx context.Context, path string, parts ...string) context.Context {
	return context.WithValue(ctx, "errors_path", PathJoin(path, parts...))
}

func Path(ctx context.Context) string {
	v, ok := ctx.Value("errors_path").(string)
	if ok {
		return v
	}
	return ""
}
