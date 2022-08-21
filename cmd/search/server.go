package main

import (
	"context"
	"net"
	"net/http"

	"github.com/cyradin/search/internal/api"
	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/storage"
	"github.com/go-chi/chi/v5"
	"github.com/go-redis/redis/v9"
)

func initServer(ctx context.Context, address string, redisClient *redis.Client, redisPrefix string) *http.Server {
	indexstorage := storage.NewDictStorage[index.IndexData](redisClient).WithPrefix(redisPrefix)

	indexRepository, err := index.NewRepository(indexstorage)
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
