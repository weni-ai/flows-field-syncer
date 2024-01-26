package configs

import (
	"log"

	"github.com/joeshaw/envdecode"
)

var config *Config

type Config struct {
	FlowsDB                string `env:"FLOWS_DB,default=postgres://temba:temba@localhost/temba?sslmode=disable"`
	MongoURI               string `env:"MONGO_URI,default=mongodb://localhost:27017"`
	MongoDbName            string `env:"MONGO_DB_NAME,default=flows-field-syncer"`
	MongoConnectionTimeout int64  `env:"MONGO_CONNECTION_TIMEOUT,default=15"`
	HostAPI                string `env:"HOST_API,default=:"`
	PortAPI                string `env:"PORT_API,default=8080"`
	SentryDSN              string `env:"SENTRY_DSN"`
	LogLevel               string `env:"LOG_LEVEL,default=debug"`
	AuthToken              string `env:"AUTH_TOKEN,default=''"`
	BatchSize              int    `env:"BATCH_SIZE,default=999"`
}

func GetConfig() *Config {
	if config == nil {
		config = &Config{}
		if err := envdecode.Decode(config); err != nil {
			log.Fatal("Error on decode env variables: ", err)
		}
	}
	return config
}
