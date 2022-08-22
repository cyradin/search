package main

import (
	"github.com/cyradin/search/internal/env"
	"github.com/cyradin/search/internal/storage"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap/zapcore"
)

type environment string

const dev environment = "dev"
const test environment = "test"

type Config struct {
	Env    string `required:"false"`
	Debug  DebugConfig
	Server ServerConfig
	Logger LoggerConfig
	Redis  storage.RedisConfig
}

type DebugConfig struct {
	ProfileMem bool `envconfig:"DEBUG_PROFILE_MEM" required:"true" default:"false"`
	ProfileCPU bool `envconfig:"DEBUG_PROFILE_CPU" required:"true" default:"false"`
}

type ServerConfig struct {
	Address string `envconfig:"SERVER_ADDRESS" required:"true" default:":8100" split_words:"true"`
}

type LoggerConfig struct {
	Level      zapcore.Level `envconfig:"LOGGER_LEVEL" required:"true" default:"info"`
	TraceLevel zapcore.Level `envconfig:"LOGGER_TRACE_LEVEL" required:"true" default:"error"`
}

func initConfig() (Config, error) {
	appEnv, err := env.Load()
	if err != nil {
		return Config{}, err
	}

	cfg := new(Config)
	err = envconfig.Process("SEARCH", cfg)
	cfg.Env = appEnv

	return *cfg, err
}
