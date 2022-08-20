package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/logger"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/pkg/profile"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	cfg, err := initConfig()
	if err != nil {
		panic(err)
	}

	if cfg.Debug.ProfileCPU {
		defer profile.Start().Stop()
	} else if cfg.Debug.ProfileMem {
		defer profile.Start(profile.MemProfile).Stop()
	}

	instanceID := uuid.NewString()
	log, err := logger.New(cfg.Logger.Level, cfg.Logger.TraceLevel, "search", version, instanceID, cfg.Env)
	panicOnError(err)

	ctx := logger.WithLogger(context.Background(), log)
	defer panicHandle(ctx, log)

	stopCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT)
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	panicOnError(redisClient.Ping(ctx).Err())

	srv := initServer(ctx, cfg.Server.Address, redisClient, cfg.Redis.KeyPrefix)

	errors := make(chan error, 1)
	go func(ctx context.Context) {
		defer panicHandle(ctx, log)
		log.Info("app.server.start", logger.ExtractFields(ctx)...)
		err := srv.ListenAndServe()
		if err != nil {
			errors <- err
		}
	}(ctx)

	select {
	case <-stopCtx.Done():
		log.Info("app.stopping", logger.ExtractFields(ctx)...)
		events.Dispatch(ctx, events.NewAppStop())

		log.Info("server.stopping", logger.ExtractFields(ctx)...)
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil && err != context.Canceled {
			log.Error("server.error", logger.ExtractFields(ctx)...)
		}
		if err := srv.Close(); err != nil {
			log.Error("server.error", logger.ExtractFields(ctx)...)
		}
		log.Info("server.stopped", logger.ExtractFields(ctx)...)
	case err := <-errors:
		log.Error("server.error", logger.ExtractFields(ctx, zap.Error(err))...)
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func panicHandle(ctx context.Context, l *zap.Logger) {
	if r := recover(); r != nil {
		err, ok := r.(error)
		if !ok {
			err = fmt.Errorf("%v", r)
		}

		l.Fatal("app.panic", logger.ExtractFields(ctx, zap.Error(err))...)
	}
}
