package main

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

func initServer(address string, h http.Handler) *http.Server {
	server := &http.Server{
		Addr:    address,
		Handler: h,
	}

	return server
}

func initHttpHandler(ctx context.Context, l *zap.Logger, v *validator.Validate) http.Handler {
	return chi.NewMux() // @todo
}
