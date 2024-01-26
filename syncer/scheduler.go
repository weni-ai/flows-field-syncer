package syncer

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/scheduler"
)

type syncerSchedulerM struct {
	logRepo  SyncerLogRepository
	confRepo SyncerConfRepository
	flowsDB  *sqlx.DB

	TaskScheduler *scheduler.Scheduler
	Syncers       map[string]Syncer
	SyncerTasks   map[string]func()
}

func NewSyncerSchedulerM(logRepo SyncerLogRepository, confRepo SyncerConfRepository, flowsDB *sqlx.DB) SyncerScheduler {
	return &syncerSchedulerM{
		Syncers:       make(map[string]Syncer),
		SyncerTasks:   make(map[string]func()),
		TaskScheduler: scheduler.NewScheduler(),

		logRepo:  logRepo,
		confRepo: confRepo,
		flowsDB:  flowsDB,
	}
}

func (s *syncerSchedulerM) StartLogCleaner() error {
	s.TaskScheduler.AddTask(
		"cleaner",
		[]scheduler.ScheduleTime{
			scheduler.ScheduleTime("01:00"),
		},
		func() {
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
		taskExecution := func() {
			start := time.Now()
			logMsg := fmt.Sprintf("start sync contact fields task at %s for syncer: %s, of type %s", start, sc.GetConfig().Service.Name, sc.GetConfig().Service.Type)
			slog.Info(logMsg)
			newLog := NewSyncerLog(
				sc.GetConfig().SyncRules.OrgID,
				sc.GetConfig().ID,
				logMsg,
				LogTypeInfo,
			)
			err := s.logRepo.Create(*newLog)
			if err != nil {
				slog.Error("Failed to create start info log: ", "err", err)
			}

			synched, err := sc.SyncContactFields(s.flowsDB)
			if err != nil {
				slog.Error("Failed to sync contact fields", "err", err)
				newLog := NewSyncerLog(
					sc.GetConfig().SyncRules.OrgID,
					sc.GetConfig().ID,
					err,
					LogTypeError,
				)
				err := s.logRepo.Create(*newLog)
				if err != nil {
					slog.Error("Failed to create error log: ", "err", err)
				}
			}
			slog.Info(fmt.Sprintf("synced %d, elapsed %s", synched, time.Since(start).String()))
		}

		s.TaskScheduler.AddTask(
			sc.GetConfig().ID,
			[]scheduler.ScheduleTime{
				scheduler.ScheduleTime(
					sc.GetConfig().SyncRules.ScheduleTime,
				),
			},
			taskExecution,
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

	taskExec := func() {
		start := time.Now()
		logMsg := fmt.Sprintf("start sync contact fields task at %s for syncer: %s, of type %s", start, newSyncer.GetConfig().Service.Name, newSyncer.GetConfig().Service.Type)
		slog.Info(logMsg)
		newLog := NewSyncerLog(
			newSyncer.GetConfig().SyncRules.OrgID,
			newSyncer.GetConfig().ID,
			logMsg,
			LogTypeInfo,
		)
		err := s.logRepo.Create(*newLog)
		if err != nil {
			slog.Error("Failed to create start info log: ", "err", err)
		}
		synched, err := newSyncer.SyncContactFields(s.flowsDB)
		if err != nil {
			slog.Error("Failed to sync contact fields", "err", err)
			newLog := NewSyncerLog(
				newSyncer.GetConfig().SyncRules.OrgID,
				newSyncer.GetConfig().ID,
				err,
				LogTypeError,
			)
			err := s.logRepo.Create(*newLog)
			if err != nil {
				slog.Error("Failed to create error log: ", "err", err)
			}
		}
		slog.Info(fmt.Sprintf("synced %d, elapsed %s", synched, time.Since(start).String()))
	}

	s.TaskScheduler.AddTask(
		newSyncer.GetConfig().ID,
		[]scheduler.ScheduleTime{
			scheduler.ScheduleTime(
				newSyncer.GetConfig().SyncRules.ScheduleTime,
			),
		},
		taskExec,
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
}

type syncerScheduler struct {
	logRepo  SyncerLogRepository
	confRepo SyncerConfRepository
	flowsDB  *sqlx.DB

	JobScheduler *gocron.Scheduler
	Syncers      map[string]Syncer
	SyncerJobs   map[string]*gocron.Job
}

func NewSyncerScheduler(logRepo SyncerLogRepository, confRepo SyncerConfRepository, flowsDB *sqlx.DB) SyncerScheduler {
	return &syncerScheduler{
		Syncers:      make(map[string]Syncer),
		SyncerJobs:   make(map[string]*gocron.Job),
		JobScheduler: gocron.NewScheduler(time.UTC),
		logRepo:      logRepo,
		confRepo:     confRepo,
		flowsDB:      flowsDB,
	}
}

func (s *syncerScheduler) StartLogCleaner() error {
	_, err := s.JobScheduler.Every(1).
		Day().
		At("00:00").
		Do(func() {
			retentionPeriod := 5 * 24 * time.Hour // 5 days
			currentTime := time.Now()
			retentionLimit := currentTime.Add(-retentionPeriod)

			deletedCount, err := s.logRepo.DeleteOlderThan(retentionLimit)
			if err != nil {
				slog.Error("Error on delete older logs:", "err", err)
			} else {
				slog.Info(fmt.Sprintf("Deleted %d logs older than %s\n", deletedCount, retentionLimit))
			}
		})
	if err != nil {
		slog.Error("error on start log cleaner task", "err", err)
		return err
	}
	s.JobScheduler.StartAsync()
	return nil
}

func (s *syncerScheduler) LoadSyncers() error {
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

func (s *syncerScheduler) StartSyncers() error {
	for _, sc := range s.Syncers {
		startTime := sc.GetConfig().SyncRules.ScheduleTime
		job, err := s.JobScheduler.Every(1).Day().At(startTime).Do(
			func() {
				start := time.Now()
				logMsg := fmt.Sprintf("start sync contact fields task at %s for syncer: %s, of type %s", start, sc.GetConfig().Service.Name, sc.GetConfig().Service.Type)
				slog.Info(logMsg)
				newLog := NewSyncerLog(
					sc.GetConfig().SyncRules.OrgID,
					sc.GetConfig().ID,
					logMsg,
					LogTypeInfo,
				)
				err := s.logRepo.Create(*newLog)
				if err != nil {
					slog.Error("Failed to create start info log: ", "err", err)
				}

				synched, err := sc.SyncContactFields(s.flowsDB)
				if err != nil {
					slog.Error("Failed to sync contact fields", "err", err)
					newLog := NewSyncerLog(
						sc.GetConfig().SyncRules.OrgID,
						sc.GetConfig().ID,
						err,
						LogTypeError,
					)
					err := s.logRepo.Create(*newLog)
					if err != nil {
						slog.Error("Failed to create error log: ", "err", err)
					}
				}
				slog.Info(fmt.Sprintf("synced %d, elapsed %s", synched, time.Since(start).String()))
			})
		if err != nil {
			slog.Error(
				fmt.Sprintf("Error on create sync job to %s with id %s",
					sc.GetConfig().Service.Name,
					sc.GetConfig().ID),
				"err", err)
		} else {
			s.SyncerJobs[sc.GetConfig().ID] = job
		}
	}
	slog.Info(fmt.Sprintf("%d Syncer started", len(s.SyncerJobs)))
	return nil
}

func (s *syncerScheduler) RegisterSyncer(scf SyncerConf) error {
	newSyncer, err := NewSyncer(scf)
	if err != nil {
		return err
	}

	task := func() {
		start := time.Now()
		logMsg := fmt.Sprintf("start sync contact fields task at %s for syncer: %s, of type %s", start, newSyncer.GetConfig().Service.Name, newSyncer.GetConfig().Service.Type)
		slog.Info(logMsg)
		newLog := NewSyncerLog(
			newSyncer.GetConfig().SyncRules.OrgID,
			newSyncer.GetConfig().ID,
			logMsg,
			LogTypeInfo,
		)
		err := s.logRepo.Create(*newLog)
		if err != nil {
			slog.Error("Failed to create start info log: ", "err", err)
		}
		synched, err := newSyncer.SyncContactFields(s.flowsDB)
		if err != nil {
			slog.Error("Failed to sync contact fields", "err", err)
			newLog := NewSyncerLog(
				newSyncer.GetConfig().SyncRules.OrgID,
				newSyncer.GetConfig().ID,
				err,
				LogTypeError,
			)
			err := s.logRepo.Create(*newLog)
			if err != nil {
				slog.Error("Failed to create error log: ", "err", err)
			}
		}
		slog.Info(fmt.Sprintf("synced %d, elapsed %s", synched, time.Since(start).String()))
	}

	stime, err := time.Parse("15:04", newSyncer.GetConfig().SyncRules.ScheduleTime)
	if err != nil {
		return err
	}

	currtime, _ := time.Parse("15:04", time.Now().Format("15:04"))
	if stime.Compare(currtime) == 0 {
		go task()
	} else {
		newJob, err := s.JobScheduler.
			Every(1).
			Day().
			At(newSyncer.GetConfig().SyncRules.ScheduleTime).
			Do(func() {
				go task()
			})
		if err != nil {
			slog.Error(
				fmt.Sprintf("Error on create sync job to %s with id %s",
					newSyncer.GetConfig().Service.Name,
					newSyncer.GetConfig().ID),
				"err", err)
			return err
		} else {
			s.SyncerJobs[newSyncer.GetConfig().ID] = newJob
		}
	}
	s.JobScheduler.Stop()
	s.JobScheduler.StartAsync()
	return nil
}

func (s *syncerScheduler) UnregisterSyncer(scf SyncerConf) error {
	err := s.JobScheduler.RemoveByID(s.SyncerJobs[scf.ID])
	if err != nil {
		return err
	}
	s.SyncerJobs[scf.ID] = nil
	return nil
}
