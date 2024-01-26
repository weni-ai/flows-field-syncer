package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/models"
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

type Syncer interface {
	GetLastModified() (time.Time, error)
	SyncContactFields(*sqlx.DB) (int, error)
	GenerateSelectToSyncQuery(int, int) (string, error)
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

func performSync(conf SyncerConf, db *sqlx.DB, results []map[string]any) (int, error) {
	var updated int
	// for each contact
	for _, r := range results {
		// slog.Info(fmt.Sprint(r))
		// update fields
		for _, v := range conf.Table.Columns {
			resultValue := r[v.Name]
			found := true
			// get contact field from flows
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			field, err := models.GetContactFieldByOrgAndKey(ctx, db, conf.SyncRules.OrgID, v.FieldMapName)
			if err != nil {
				slog.Error(fmt.Sprintf("field could not be found in flows. field: %s", v.Name), "err", err)
				found = false
			}
			if found {
				// if field exists in flows, update that field in contact
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				switch conf.Table.RelationType {
				case RelationTypeContact:
					err := models.UpdateContactField(ctx, db, r[conf.Table.RelationColumn].(string), field.UUID, resultValue)
					if err != nil {
						errMsg := fmt.Sprintf("field could not be updated: %v", field)
						slog.Error(errMsg, "err", err)
						return updated, errors.Wrap(err, errMsg)
					}
				case RelationTypeURN:
					err := models.UpdateContactFieldByURN(ctx, db, r[conf.Table.RelationColumn].(string), conf.SyncRules.OrgID, field.UUID, resultValue)
					if err != nil {
						errMsg := fmt.Sprintf("field could not be updated: %v", field)
						slog.Error(errMsg, "err", err)
						return updated, errors.Wrap(err, errMsg)
					}
				}
			} else {
				// if field not exist, create it and create it in contact field column jsonb
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cf := models.NewContactField(
					v.FieldMapName,
					v.FieldMapName,
					models.ScanValueType(resultValue),
					conf.SyncRules.OrgID,
					conf.SyncRules.AdminID,
					conf.SyncRules.AdminID)

				err := models.CreateContactField(ctx, db, cf)
				if err != nil {
					errMsg := fmt.Sprintf("error creating contact field: %v", v.FieldMapName)
					slog.Error(errMsg, "err", err)
					return updated, errors.Wrap(err, errMsg)
				} else {
					switch conf.Table.RelationType {
					case RelationTypeContact:
						err := models.UpdateContactField(ctx, db, r[conf.Table.RelationColumn].(string), cf.UUID, resultValue)
						if err != nil {
							errMsg := fmt.Sprintf("error updating contact field: %v", v.FieldMapName)
							slog.Error(errMsg, "err", err)
							return updated, errors.Wrap(err, errMsg)
						}
					case RelationTypeURN:
						err := models.UpdateContactFieldByURN(ctx, db, r[conf.Table.RelationColumn].(string), conf.SyncRules.OrgID, cf.UUID, resultValue)
						if err != nil {
							errMsg := fmt.Sprintf("error updating contact field: %v", v.FieldMapName)
							slog.Error(errMsg, "err", err)
							return updated, errors.Wrap(err, errMsg)
						}
					}
				}
			}
		}
		updated++
	}
	return updated, nil
}
