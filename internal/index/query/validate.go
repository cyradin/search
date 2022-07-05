package query

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func errorParams(path string) map[string]interface{} {
	return map[string]interface{}{
		"path": path,
	}
}

func errorRequired(ctx context.Context) validation.Error {
	return validation.NewError("validation_required", "cannot be blank").
		SetParams(errorParams(path(ctx)))
}

func errorSingleKeyRequired(ctx context.Context) validation.Error {
	return validation.NewError("validation_length_invalid", "the length must be exactly 1").
		SetParams(errorParams(path(ctx)))
}

func errorArrayRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_array_required", "must be an array").
		SetParams(errorParams(pathJoin(path(ctx), key)))
}

func errorObjectRequired(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_object_required", "must be an object").
		SetParams(errorParams(pathJoin(path(ctx), key)))
}

func errorUnknownQueryType(ctx context.Context, key string) validation.Error {
	return validation.NewError("validation_unknown_query_type", "unknown query type").
		SetParams(errorParams(pathJoin(path(ctx), key)))
}
