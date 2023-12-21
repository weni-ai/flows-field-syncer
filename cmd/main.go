package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/getsentry/sentry-go"
	slogsentry "github.com/ihippik/slog-sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/configs"
	"github.weni-ai/flows-field-syncer/syncer"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	config := configs.NewConfig()
	if config.SentryDSN != "" {
		initLogger(config)
		defer sentry.Flush(2 * time.Second)
	}

	flowsdb, err := initFlowsDB(config)
	if err != nil {
		slog.Error("error on initalize flows db", "err", err)
		os.Exit(1)
	}

	syncerdb, err := initMongo(config)
	if err != nil {
		slog.Error("error on initialize mongo db", "err", err)
		os.Exit(1)
	}

	syncerConfRepo := syncer.NewSyncerConfRepository(syncerdb)
	syncerLogRepo := syncer.NewSyncerLogRepository(syncerdb)

	api := syncer.NewSyncerAPI(
		config,
		syncerConfRepo,
	)
	api.Start()
	slog.Info("syncer api started")

	sc := syncer.NewSyncerScheduler(
		syncerLogRepo,
		syncerConfRepo,
		flowsdb,
	)

	err = sc.StartLogCleaner()
	if err != nil {
		log.Fatal(errors.Wrap(err, "error on start log cleaning"))
	}

	err = sc.LoadSyncers()
	if err != nil {
		log.Fatal(errors.Wrap(err, "error on load syncers"))
	}

	err = sc.StartSyncers()
	if err != nil {
		slog.Error("error on start syncers", "err", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := api.Server.Shutdown(ctx); err != nil {
		slog.Error("error on shutdown api", "err", err)
	}
	flowsdb.Close()
	syncerdb.Client().Disconnect(context.Background())
	slog.Info("flows-field-syncer stopped")
}

func initLogger(config *configs.Config) {
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              config.SentryDSN,
		EnableTracing:    false,
		AttachStacktrace: true,
	})
	if err != nil {
		log.Fatal("sentry init error", err)
	}
	loglevel, ok := map[string]slog.Leveler{
		"debug":   slog.LevelDebug,
		"error":   slog.LevelError,
		"info":    slog.LevelInfo,
		"warning": slog.LevelWarn,
	}[config.LogLevel]
	if !ok {
		loglevel = slog.LevelDebug
	}
	opt := slog.HandlerOptions{
		Level: loglevel,
	}
	handler := slog.NewTextHandler(os.Stdout, &opt)
	hook := slogsentry.NewSentryHandler(handler, []slog.Level{slog.LevelWarn, slog.LevelError})
	logger := slog.New(hook)
	slog.SetDefault(logger)
}

func initMongo(config *configs.Config) (*mongo.Database, error) {
	mongoClientOptions := options.Client().ApplyURI(config.MongoURI)
	ctx, ctxCancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer ctxCancel()

	mongoClient, err := mongo.Connect(ctx, mongoClientOptions)
	if err != nil {
		return nil, errors.Wrap(err, "error on connect to mongo")
	}
	if err := mongoClient.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, errors.Wrap(err, "mongodb fail to ping")
	} else {
		slog.Info("mongodb OK")
	}
	syncerdb := mongoClient.Database(config.MongoDbName)
	return syncerdb, nil
}

func initFlowsDB(config *configs.Config) (*sqlx.DB, error) {
	flowsdb, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		return nil, errors.Wrap(err, "error on open flows db")
	}

	if err = flowsdb.Ping(); err != nil {
		return nil, errors.Wrap(err, "error on ping flows db")
	}
	return flowsdb, nil
}
