package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsm/redislock"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.weni-ai/flows-field-syncer/scheduler"
)

const minIntervalLock = time.Hour * 1

type syncerSchedulerM struct {
	logRepo  SyncerLogRepository
	confRepo SyncerConfRepository
	flowsDB  *sqlx.DB
	redis    *redis.Client
	locker   *redislock.Client

	TaskScheduler *scheduler.Scheduler
	Syncers       map[string]Syncer
	SyncerTasks   map[string]func()
}

func NewSyncerSchedulerM(logRepo SyncerLogRepository, confRepo SyncerConfRepository, flowsDB *sqlx.DB, redis *redis.Client, locker *redislock.Client) SyncerScheduler {
	return &syncerSchedulerM{
		Syncers:       make(map[string]Syncer),
		SyncerTasks:   make(map[string]func()),
		TaskScheduler: scheduler.NewScheduler(),

		redis:  redis,
		locker: locker,

		logRepo:  logRepo,
		confRepo: confRepo,
		flowsDB:  flowsDB,
	}
}

func (s *syncerSchedulerM) StartLogCleaner() error {
	taskKey := "cleaner"
	s.TaskScheduler.AddTask(
		taskKey,
		[]scheduler.ScheduleTime{
			scheduler.ScheduleTime("01:00"),
		},
		func() {
			ctx := context.Background()
			lock, err := s.locker.Obtain(ctx, taskKey, minIntervalLock, nil)
			if err == redislock.ErrNotObtained {
				slog.Info(fmt.Sprintf("%s task still in progress", taskKey))
				return
			} else if err != nil {
				slog.Error("Could not obtain lock!", "err", err)
				return
			}
			defer lock.Release(ctx)
			retentionPeriod := 5 * 24 * time.Hour // 5 days
			currentTime := time.Now()
			retentionLimit := currentTime.Add(-retentionPeriod)

			deletedCount, err := s.logRepo.DeleteOlderThan(retentionLimit)
			if err != nil {
				slog.Error("Error on delete older logs:", "err", err)
			} else {
				slog.Info(fmt.Sprintf("Deleted %d logs older than %s\n", deletedCount, retentionLimit))
			}
		},
	)
	return nil
}

func (s *syncerSchedulerM) LoadSyncers() error {
	loadedSyncers := make(map[string]Syncer)
	confs, err := s.confRepo.GetAll()
	if err != nil {
		return errors.Wrap(err, "error on get all syncers")
	}
	for _, cf := range confs {
		sc, err := NewSyncer(cf)
		if err != nil {
			slog.Error("error on instantiate syncer", "err", err)
		} else {
			loadedSyncers[cf.ID] = sc
		}
	}
	s.Syncers = loadedSyncers
	return nil
}

func (s *syncerSchedulerM) StartSyncers() error {
	for _, sc := range s.Syncers {
		scheduleTimes := []scheduler.ScheduleTime{}
		for _, st := range sc.GetConfig().SyncRules.ScheduleTimes {
			scheduleTimes = append(scheduleTimes, scheduler.ScheduleTime(st))
		}

		s.TaskScheduler.AddTask(
			sc.GetConfig().ID,
			scheduleTimes,
			func() {
				s.syncerTask(sc)
			},
		)
	}

	s.TaskScheduler.Start()
	return nil
}

func (s *syncerSchedulerM) RegisterSyncer(scf SyncerConf) error {
	newSyncer, err := NewSyncer(scf)
	if err != nil {
		return err
	}

	scheduleTimes := []scheduler.ScheduleTime{}
	for _, st := range newSyncer.GetConfig().SyncRules.ScheduleTimes {
		scheduleTimes = append(scheduleTimes, scheduler.ScheduleTime(st))
	}

	s.TaskScheduler.AddTask(
		newSyncer.GetConfig().ID,
		scheduleTimes,
		func() {
			s.syncerTask(newSyncer)
		},
	)

	return nil
}

func (s *syncerSchedulerM) UnregisterSyncer(scf SyncerConf) error {
	s.TaskScheduler.RemoveTask(scf.ID)
	return nil
}

type SyncerScheduler interface {
	StartLogCleaner() error
	LoadSyncers() error
	StartSyncers() error
	RegisterSyncer(SyncerConf) error
	UnregisterSyncer(SyncerConf) error
	Close() error
}

func (s *syncerSchedulerM) Close() error {
	s.TaskScheduler.Stop()
	return nil
}

func (s *syncerSchedulerM) syncerTask(syncer Syncer) {
	ctx := context.Background()
	taskKey := syncer.GetConfig().ID
	lock, err := s.locker.Obtain(ctx, taskKey, minIntervalLock, nil)
	if err == redislock.ErrNotObtained {
		slog.Info(fmt.Sprintf("%s sync still in progress", taskKey))
		return
	} else if err != nil {
		slog.Error("Could not obtain lock!", "err", err)
		return
	}
	defer lock.Release(ctx)

	start := time.Now()
	logMsg := fmt.Sprintf("start sync contact fields task at %s for syncer: %s(%s), of type %s",
		start, syncer.GetConfig().ID, syncer.GetConfig().Service.Name, syncer.GetConfig().Service.Type)
	slog.Info(logMsg)
	newLog := NewSyncerLog(
		syncer.GetConfig().SyncRules.OrgID,
		syncer.GetConfig().ID,
		logMsg,
		LogTypeInfo,
	)
	err = s.logRepo.Create(*newLog)
	if err != nil {
		slog.Error("Failed to create start info log: ", "err", err)
	}

	var synched int
	switch syncer.GetConfig().SyncRules.Strategy {
	case StrategyTypeContactURN:
		ctx := context.Background()
		synched, err = SyncContactFieldsStrategy2(ctx, s.flowsDB, syncer)
	case StrategyTypePull:
		synched, err = SyncContactFields(s.flowsDB, syncer)
	default:
		synched, err = SyncContactFields(s.flowsDB, syncer)
	}
	if err != nil {
		slog.Info("Failed to sync")
		slog.Error("Failed to sync contact fields", "err", err)
		newLog := NewSyncerLog(
			syncer.GetConfig().SyncRules.OrgID,
			syncer.GetConfig().ID,
			err,
			LogTypeError,
		)
		err := s.logRepo.Create(*newLog)
		if err != nil {
			slog.Error("Failed to create error log: ", "err", err)
		}
	}
	slog.Info(
		fmt.Sprintf("syncer %s(%s), synced %d, elapsed %s",
			syncer.GetConfig().ID, syncer.GetConfig().Service.Name, synched, time.Since(start).String()))
}
