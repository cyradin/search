package main

import (
	"context"
	"os"
	"path"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/storage"
)

const dataDir = ".data"
const dirPermissions = 0755
const filePermissions = 0644

func initIndexes(ctx context.Context) *index.Repository {
	panicOnError(os.MkdirAll(dataDir, dirPermissions))

	indexStorage, err := storage.NewFile[*index.Index](ctx, path.Join(dataDir, "indexes.json"))
	panicOnError(err)

	indexRepository := index.NewRepository(ctx, indexStorage)

	return indexRepository
}
