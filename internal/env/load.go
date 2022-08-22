package env

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

const (
	EnvVar  = "APP_ENV"
	EnvDev  = "dev"
	EnvTest = "test"
)

func Load() (appEnv string, err error) {
	appEnv = os.Getenv(EnvVar)
	if appEnv == "" {
		appEnv = string(EnvDev)
		os.Setenv(EnvVar, appEnv)
	}

	err = loadEnvFile(fmt.Sprintf(".env.%s.local", appEnv))
	if err != nil {
		return
	}

	if appEnv != string(EnvTest) {
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
