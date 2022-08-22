package storage

import (
	"context"
	"os"
	"testing"

	"github.com/cyradin/search/internal/env"
	"github.com/go-redis/redis/v9"
	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/require"
)

func testContext(t *testing.T) context.Context {
	redisCfg := RedisConfig{}

	err := envconfig.Process("", &redisCfg)
	require.NoError(t, err)

	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Addr,
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
	})

	resp := c.FlushAll(ctx)
	require.NoError(t, resp.Err())

	return WithRedis(context.Background(), c)
}

func TestMain(m *testing.M) {
	// go to the root dir to apply .env files
	if err := os.Chdir("../.."); err != nil {
		panic(err)
	}

	os.Setenv("APP_ENV", "test")
	_, err := env.Load()
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}
