package main

import (
	"context"
	"os"
	"path"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/storage"
	"github.com/cyradin/search/pkg/finisher"
)

const dataDir = "/home/user/app/.data"
const dirPermissions = 0755
const filePermissions = 0644

func initIndexes(ctx context.Context) *index.Repository {
	panicOnError(os.MkdirAll(dataDir, dirPermissions))

	indexStorage, err := storage.NewFile[*index.Index](path.Join(dataDir, "indexes.json"))
	panicOnError(err)
	finisher.Add(indexStorage)

	indexRepository, err := index.NewRepository(ctx, indexStorage, dataDir)
	panicOnError(err)

	return indexRepository
}
