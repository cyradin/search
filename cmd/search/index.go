package main

import (
	"github.com/cyradin/search/internal/index"
)

const dataDir = "/home/user/app/.data"

func initIndexes() *index.Repository {
	indexRepository, err := index.NewRepository(dataDir)
	panicOnError(err)

	return indexRepository
}
