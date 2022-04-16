package main

import (
	"context"
	"net/http"

	"github.com/cyradin/search/internal/apiv1"
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
	mux := chi.NewMux()
	mux.Route("/v1", apiv1.NewHandler(ctx))

	return mux, nil
}
