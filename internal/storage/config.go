package storage

type RedisConfig struct {
	Addr      string `envconfig:"REDIS_ADDR" required:"true"`
	Password  string `envconfig:"REDIS_PASSWORD" require:"false"`
	DB        int    `envconfig:"REDIS_DB" require:"false"`
	KeyPrefix string `envconfig:"REDIS_KEY_PREFIX" require:"true" default:"search"`
}
