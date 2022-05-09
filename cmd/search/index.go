package main

import (
	"context"

	"github.com/cyradin/search/internal/index"
)

const dataDir = "/home/user/app/.data"

func initIndexes(ctx context.Context) *index.Repository {
	indexRepository, err := index.NewRepository(ctx, dataDir)
	panicOnError(err)

	return indexRepository
}
