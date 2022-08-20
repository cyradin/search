package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
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
	Redis  RedisConfig
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

type RedisConfig struct {
	Addr      string `envconfig:"REDIS_ADDR" required:"true"`
	Password  string `envconfig:"REDIS_PASSWORD" require:"false"`
	DB        int    `envconfig:"REDIS_DB" require:"false"`
	KeyPrefix string `envconfig:"REDIS_KEY_PREFIX" require:"true" default:"search"`
}

func initConfig() (Config, error) {
	appEnv, err := loadEnv("SEARCH_ENV")
	if err != nil {
		return Config{}, err
	}

	cfg := new(Config)
	err = envconfig.Process("SEARCH", cfg)
	cfg.Env = appEnv

	return *cfg, err
}

var loadEnv = func(envVar string) (appEnv string, err error) {
	appEnv = os.Getenv(envVar)
	if appEnv == "" {
		appEnv = string(dev)
		os.Setenv(envVar, appEnv)
	}

	err = loadEnvFile(fmt.Sprintf(".env.%s.local", appEnv))
	if err != nil {
		return
	}

	if appEnv != string(test) {
		err = loadEnvFile(".env.local")
		if err != nil {
			return
		}
	}

	err = loadEnvFile(fmt.Sprintf(".env.%s", appEnv))
	if err != nil {
		return
	}

	err = loadEnvFile(".env")
	return
}

func loadEnvFile(file string) error {
	_, err := os.Stat(file)

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	err = godotenv.Load(file)
	if err != nil {
		return fmt.Errorf("failed to load %s: %w", file, err)
	}

	return err
}
