package configs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {

	t.Run("defined configs", func(t *testing.T) {

		var (
			flowsDB      = "test_flows_db"
			mongoURI     = "test_mongo_uri"
			mongoDbName  = "test_mongo_db_name"
			hostAPI      = "test_host_api"
			portAPI      = "90909"
			sentryDSB    = "test_sentry_dsn"
			authTokenAPI = "test_auth_token_api"
			batchSize    = "batch_size"
		)
		os.Setenv("FLOWS_DB", flowsDB)
		os.Setenv("MONGO_URI", mongoURI)
		os.Setenv("MONGO_DB_NAME", mongoDbName)
		os.Setenv("HOST_API", hostAPI)
		os.Setenv("PORT_API", portAPI)
		os.Setenv("SENTRY_DSN", sentryDSB)
		os.Setenv("AUTH_TOKEN", authTokenAPI)
		os.Setenv("BATCH_SIZE", batchSize)

		defer func() {
			os.Unsetenv("FLOWS_DB")
			os.Unsetenv("MONGO_URI")
			os.Unsetenv("MONGO_DB_NAME")
			os.Unsetenv("HOST_API")
			os.Unsetenv("PORT_API")
			os.Unsetenv("SENTRY_DSN")
			os.Unsetenv("AUTH_TOKEN")
			os.Unsetenv("BATCH_SIZE")
		}()

		config := GetConfig()

		assert.Equal(t, flowsDB, config.FlowsDB)
		assert.Equal(t, mongoURI, config.MongoURI)
		assert.Equal(t, mongoDbName, config.MongoDbName)
		assert.Equal(t, hostAPI, config.HostAPI)
		assert.Equal(t, portAPI, config.PortAPI)
		assert.Equal(t, sentryDSB, config.SentryDSN)
		assert.Equal(t, authTokenAPI, config.AuthToken)
		assert.Equal(t, batchSize, config.BatchSize)
	})

	t.Run("default values", func(t *testing.T) {
		var (
			flowsDB     = "postgres://temba:temba@localhost/temba?sslmode=disable"
			mongoURI    = "mongodb://localhost:27017"
			mongoDbName = "flows-field-syncer"
			hostAPI     = ":"
			portAPI     = "8080"
			sentryDSB   = ""
			authToken   = ""
			batchSize   = 999
		)
		config := GetConfig()

		assert.Equal(t, flowsDB, config.FlowsDB)
		assert.Equal(t, mongoURI, config.MongoURI)
		assert.Equal(t, mongoDbName, config.MongoDbName)
		assert.Equal(t, hostAPI, config.HostAPI)
		assert.Equal(t, portAPI, config.PortAPI)
		assert.Equal(t, sentryDSB, config.SentryDSN)
		assert.Equal(t, authToken, config.SentryDSN)
		assert.Equal(t, batchSize, config.BatchSize)
	})
}
