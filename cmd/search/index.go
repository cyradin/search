package main

import (
	"context"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/storage"
)

const dataDir = "/home/user/app/.data"
const dirPermissions = 0755
const filePermissions = 0644

func initIndexes(ctx context.Context) *index.Repository {
	storageFactory, err := storage.NewFactory(storage.FileDriver, storage.FileConfig{Dir: dataDir})
	panicOnError(err)

	indexRepository, err := index.NewRepository(ctx, storageFactory, dataDir)
	panicOnError(err)

	return indexRepository
}
