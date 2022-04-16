package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyradin/search/pkg/logger"
	"github.com/google/uuid"
	"github.com/pkg/profile"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	l := initLogger(cfg.Logger.Level, cfg.Logger.TraceLevel, instanceID, cfg.Env)
	ctx := logger.WithContext(context.Background(), l)
	defer panicHandle(ctx, l)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	h, err := initHttpHandler(ctx)
	panicOnError(err)

	srv := initServer(cfg.Server.Address, h)

	errors := make(chan error, 1)
	go func(ctx context.Context) {
		defer panicHandle(ctx, l)
		l.Info("app.server.start", logger.ExtractFields(ctx)...)
		err := srv.ListenAndServe()
		if err != nil {
			errors <- err
		}
	}(ctx)

	select {
	case <-ctx.Done():
		l.Info("app.server.stopping", logger.ExtractFields(ctx)...)
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil && err != context.Canceled {
			l.Error("app.server.error", logger.ExtractFields(ctx)...)
		}
		if err := srv.Close(); err != nil {
			l.Error("app.server.error", logger.ExtractFields(ctx)...)
		}
		l.Info("app.server.stopped", logger.ExtractFields(ctx)...)
	case err := <-errors:
		l.Error("app.server.error", logger.ExtractFields(ctx, zap.Error(err))...)
	}
}

func initLogger(level zapcore.Level, traceLevel zapcore.Level, instanceID string, env string) *zap.Logger {
	l, err := logger.New(level, traceLevel, "search", version, instanceID, env)
	panicOnError(err)

	return l
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
