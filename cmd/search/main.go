package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyradin/search/pkg/ctxt"
	"github.com/cyradin/search/pkg/finisher"
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

	if cfg.Debug.ProfileCpu {
		defer profile.Start().Stop()
	} else if cfg.Debug.ProfileMem {
		defer profile.Start(profile.MemProfile).Stop()
	}

	instanceID := uuid.NewString()
	logger, err := newLogger(cfg.Logger.Level, cfg.Logger.TraceLevel, "search", version, instanceID, cfg.Env)
	panicOnError(err)

	ctx := ctxt.WithLogger(context.Background(), logger)
	defer panicHandle(ctx, logger)

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT)
	defer cancel()

	h, err := initHttpHandler(ctx)
	panicOnError(err)

	srv := initServer(cfg.Server.Address, h)

	errors := make(chan error, 1)
	go func(ctx context.Context) {
		defer panicHandle(ctx, logger)
		logger.Info("app.server.start", ctxt.ExtractFields(ctx)...)
		err := srv.ListenAndServe()
		if err != nil {
			errors <- err
		}
	}(ctx)

	select {
	case <-ctx.Done():
		logger.Info("app.server.stopping", ctxt.ExtractFields(ctx)...)
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil && err != context.Canceled {
			logger.Error("app.server.error", ctxt.ExtractFields(ctx)...)
		}
		if err := srv.Close(); err != nil {
			logger.Error("app.server.error", ctxt.ExtractFields(ctx)...)
		}
		logger.Info("app.server.stopped", ctxt.ExtractFields(ctx)...)
	case err := <-errors:
		logger.Error("app.server.error", ctxt.ExtractFields(ctx, zap.Error(err))...)
	}

	logger.Info("app.stopping", ctxt.ExtractFields(ctx)...)
	finisher.Wait(ctx)
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

		l.Fatal("app.panic", ctxt.ExtractFields(ctx, zap.Error(err))...)
	}
}
