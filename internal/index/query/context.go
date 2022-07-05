package query

import (
	"context"
	"strings"
)

func pathJoin(path string, parts ...string) string {
	if len(parts) > 0 {
		parts = append([]string{path}, parts...)
		path = strings.Join(parts, ".")
	}

	return path
}

func withPath(ctx context.Context, path string, parts ...string) context.Context {
	return context.WithValue(ctx, "queryPath", pathJoin(path, parts...))
}

func path(ctx context.Context) string {
	v, ok := ctx.Value("queryPath").(string)
	if ok {
		return v
	}
	return ""
}

func withFields(ctx context.Context, fields Fields) context.Context {
	return context.WithValue(ctx, "queryFields", fields)
}

func fields(ctx context.Context) Fields {
	v, ok := ctx.Value("queryFields").(Fields)
	if ok {
		return v
	}
	return nil
}
