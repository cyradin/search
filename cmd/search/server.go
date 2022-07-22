package main

import (
	"context"
	"net/http"

	"github.com/cyradin/search/internal/api"
	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
)

const dataDir = "/home/user/app/.data"

func initServer(ctx context.Context, address string) *http.Server {
	docRepository := index.NewDocuments(dataDir)
	indexRepository, err := index.NewRepository(dataDir, docRepository)
	panicOnError(err)
	err = indexRepository.Init(ctx)
	panicOnError(err)

	mux := chi.NewMux()
	mux.Route("/", api.NewHandler(ctx, indexRepository, docRepository))

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	return server
}
