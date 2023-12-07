package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	TypePostgres = "postgres"
	TypeBigQuery = "bigquery"
	TypeAthena   = "athena"
)

type Syncer interface {
	GetLastModified() (time.Time, error)
	SyncContactFields(*sqlx.DB) (int, error)
	GenerateSelectToSyncQuery() (string, error)
	MakeQuery(context.Context, string) ([]map[string]any, error)
	Close() error
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
	ID        string        `bson:"_id" json:"id"`
	Service   SyncerService `bson:"service" json:"service"`
	SyncRules struct {
		Interval int   `bson:"interval" json:"interval"`
		OrgID    int64 `bson:"org_id" json:"org_id"`
		AdminID  int64 `bson:"admin_id" json:"admin_id"`
	} `bson:"sync_rules" json:"sync_rules"`
	Table SyncerTable `bson:"table" json:"table"`
}

type SyncerService struct {
	Name   string                 `bson:"name" json:"name"`
	Type   string                 `bson:"type" json:"type"`
	Access map[string]interface{} `bson:"access" json:"access"`
}

type SyncerTable struct {
	Name             string         `bson:"name" json:"name"`
	TableDestination string         `bson:"table_destination" json:"table_destination"`
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
	CretedAt  time.Time   `bson:"creted_at" json:"creted_at"`
	UpdatedAt time.Time   `bson:"updated_at" json:"updated_at"`
}

func (c *SyncerConf) Validate() error {
	return errors.New("not implemented")
}

func LoadFileConf() []SyncerConf {
	confsFile := "../sync_confs_bq.json"
	file, err := os.Open(confsFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var confs []SyncerConf

	err = json.NewDecoder(file).Decode(&confs)
	if err != nil {
		log.Fatal(err)
	}

	return confs
}
