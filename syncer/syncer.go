package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	TypePostgres = "postgres"
	TypeBigQuery = "bigquery"
	TypeAthena   = "athena"
)

const (
	RelationTypeURN     = "urn"
	RelationTypeContact = "contact"
)

const (
	LogTypeInfo  = "info"
	LogTypeError = "error"
)

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

type Syncer interface {
	GetLastModified() (time.Time, error)
	SyncContactFields(*sqlx.DB) (int, error)
	GenerateSelectToSyncQuery() (string, error)
	MakeQuery(context.Context, string) ([]map[string]any, error)
	Close() error
	GetConfig() SyncerConf
}

func NewSyncer(conf SyncerConf) (Syncer, error) {
	switch conf.Service.Type {
	case TypePostgres:
		return NewSyncerPG(conf)
	case TypeBigQuery:
		return NewSyncerBigQuery(conf)
	case TypeAthena:
		return NewSyncerAthena(conf)
	}
	return nil, errors.New("service type not supported")
}

type SyncerConf struct {
	ID        string        `bson:"_id" json:"id,omitempty"`
	Service   SyncerService `bson:"service" json:"service"`
	SyncRules struct {
		ScheduleTime string `bson:"schedule_time" json:"schedule_time"`
		Interval     int    `bson:"interval" json:"interval"`
		OrgID        int64  `bson:"org_id" json:"org_id"`
		AdminID      int64  `bson:"admin_id" json:"admin_id"`
	} `bson:"sync_rules" json:"sync_rules"`
	Table    SyncerTable `bson:"table" json:"table"`
	IsActive bool        `bson:"is_active" json:"is_active"`
}

type SyncerService struct {
	Name   string                 `bson:"name" json:"name"`
	Type   string                 `bson:"type" json:"type"`
	Access map[string]interface{} `bson:"access" json:"access"`
}

type SyncerTable struct {
	Name             string         `bson:"name" json:"name"`
	TableDestination string         `bson:"table_destination" json:"table_destination"`
	RelationType     string         `bson:"relation_type" json:"relation_type"`
	RelationColumn   string         `bson:"relation_column" json:"relation_column"`
	Columns          []SyncerColumn `bson:"columns" json:"columns"`
}

type SyncerColumn struct {
	Name         string `bson:"name" json:"name"`
	FieldMapName string `bson:"field_map_name" json:"field_map_name"`
}

type SyncerLog struct {
	ID        string      `bson:"_id" json:"id"`
	OrgID     int64       `bson:"org_id" json:"org_id"`
	ConfID    string      `bson:"conf_id" json:"conf_id"`
	Details   interface{} `bson:"details" json:"details"`
	LogType   string      `bson:"type" json:"log_type"`
	CretedAt  time.Time   `bson:"creted_at" json:"creted_at"`
	UpdatedAt time.Time   `bson:"updated_at" json:"updated_at"`
}

func NewSyncerLog(orgID int64, confID string, details interface{}, logType string) *SyncerLog {
	return &SyncerLog{
		OrgID:     orgID,
		ConfID:    confID,
		Details:   details,
		LogType:   logType,
		CretedAt:  time.Now(),
		UpdatedAt: time.Now(),
	}
}

func (c *SyncerConf) Validate() error {
	return errors.New("not implemented")
}

type SyncerLogCleaner interface {
}
