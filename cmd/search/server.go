package main

import (
	"context"
	"net/http"

	"github.com/cyradin/search/internal/apiv1"
	"github.com/go-chi/chi/v5"
)

func initServer(ctx context.Context, address string) *http.Server {
	indexRepository := initIndexes(ctx)

	mux := chi.NewMux()
	mux.Route("/v1", apiv1.NewHandler(ctx, indexRepository))

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	return server
}
