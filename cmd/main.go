package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/go-co-op/gocron"
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
		sentry.Flush(2 * time.Second)
		os.Exit(1)
	}

	syncerdb, err := initMongo(config)
	if err != nil {
		slog.Error("error on initialize mongo db", "err", err)
		sentry.Flush(2 * time.Second)
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

	s := gocron.NewScheduler(time.UTC)
	_, err = s.Every(1).
		Day().
		At("00:00").
		Do(func() {
			retentionPeriod := 5 * 24 * time.Hour // 5 days
			currentTime := time.Now()
			retentionLimit := currentTime.Add(-retentionPeriod)

			deletedCount, err := syncerLogRepo.DeleteOlderThan(retentionLimit)
			if err != nil {
				slog.Error("Error on delete older logs:", "err", err)
			} else {
				slog.Info(fmt.Sprintf("Deleted %d logs older than %s\n", deletedCount, retentionLimit))
			}
		})
	if err != nil {
		slog.Error("error on start log cleaner task", "err", err)
	}

	go func() {
		for {
			var loadedSyncers []syncer.Syncer
			syncerConfs, err := syncerConfRepo.GetAll()
			if err != nil {
				slog.Error("error on get all syncers", "err", err)
			} else {
				for _, scf := range syncerConfs {
					sc, err := syncer.NewSyncer(scf)
					if err != nil {
						slog.Error("error on get syncer", "err", err)
					} else {
						loadedSyncers = append(loadedSyncers, sc)
					}
				}
				for _, sc := range loadedSyncers {
					start := time.Now()
					synched, err := sc.SyncContactFields(flowsdb)
					if err != nil {
						slog.Error("Failed to sync contact fields", "err", err)
						continue
					}
					slog.Info(fmt.Sprintf("synced %d, elapsed %s", synched, time.Since(start).String()))
				}
			}
			time.Sleep(time.Second * 10)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := api.Server.Shutdown(ctx); err != nil {
		slog.Error("error on shutdown api", "err", err)
	}
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
	defer flowsdb.Close()
	if err = flowsdb.Ping(); err != nil {
		return nil, errors.Wrap(err, "error on ping flows db")
	}
	return flowsdb, nil
}
