package main

import (
	"context"
	"net/http"

	"github.com/cyradin/search/internal/apiv1"
	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/storage"
	"github.com/go-chi/chi/v5"
)

func initServer(address string, h http.Handler) *http.Server {
	server := &http.Server{
		Addr:    address,
		Handler: h,
	}

	return server
}

func initHttpHandler(ctx context.Context) (http.Handler, error) {
	indexStorage, err := storage.NewFile[*index.Index](ctx, "data/indexes")
	if err != nil {
		return nil, err
	}
	indexRepository := index.NewRepository(ctx, indexStorage)

	mux := chi.NewMux()
	mux.Route("/v1", apiv1.NewHandler(ctx, indexRepository))

	return mux, nil
}
