package valid

import (
	"context"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const PathParam = "path"

func ErrParams(path string) map[string]interface{} {
	return map[string]interface{}{
		PathParam: path,
	}
}

func NewErrRequired(ctx context.Context) validation.Error {
	return validation.NewError("validation_required", "cannot be blank").
		SetParams(ErrParams(Path(ctx)))
}

func NewErrSingleKeyRequired(ctx context.Context) validation.Error {
	return validation.NewError("validation_length_invalid", "the length must be exactly 1").
		SetParams(ErrParams(Path(ctx)))
}

func NewErrArrayRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_array_required", "must be an array").
		SetParams(ErrParams(PathJoin(Path(ctx), key)))
}

func NewErrObjectRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_object_required", "must be an object").
		SetParams(ErrParams(PathJoin(Path(ctx), key)))
}

func NewErrUnknownValue(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_unknown_value", "unknown value").
		SetParams(ErrParams(PathJoin(Path(ctx), key)))
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
