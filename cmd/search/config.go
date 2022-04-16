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
	Logger LoggerConfig `required:"true" split_words:"true"`
}

type DebugConfig struct {
	ProfileMem bool `required:"true" default:"false" split_words:"true"`
	ProfileCpu bool `required:"true" default:"false" split_words:"true"`
}

type ServerConfig struct {
	Address string `required:"true" default:":8100" split_words:"true"`
}

type LoggerConfig struct {
	Level      zapcore.Level `required:"true" default:"info"`
	TraceLevel zapcore.Level `required:"true" default:"error"`
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
