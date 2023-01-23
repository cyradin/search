package main

import (
	"context"
	"net"
	"net/http"

	"github.com/cyradin/search/internal/api"
	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
)

func initServer(ctx context.Context, address string) *http.Server {
	indexRepository, err := index.NewRepository(ctx)
	panicOnError(err)
	err = indexRepository.Init(ctx)
	panicOnError(err)

	mux := chi.NewMux()
	mux.Route("/", api.NewHandler(ctx, indexRepository))

	server := &http.Server{
		Addr:    address,
		Handler: mux,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	return server
}
